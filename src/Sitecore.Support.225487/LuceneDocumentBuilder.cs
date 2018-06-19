using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using Lucene.Net.Documents;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Boosting;
using Sitecore.ContentSearch.ComputedFields;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.LuceneProvider;
using Sitecore.Diagnostics;

namespace Sitecore.Support.ContentSearch.LuceneProvider
{
  public partial class LuceneDocumentBuilder : Sitecore.ContentSearch.LuceneProvider.LuceneDocumentBuilder
  {
    private ConcurrentQueue<IFieldable> _fields = new ConcurrentQueue<IFieldable>();

    private readonly LuceneSearchFieldConfiguration defaultTextField = new LuceneSearchFieldConfiguration("NO", "TOKENIZED", "NO", 1.0f);

    private readonly LuceneSearchFieldConfiguration defaultStoreField = new LuceneSearchFieldConfiguration("NO", "TOKENIZED", "YES", 1.0f);

    private readonly IProviderUpdateContext Context;

    public new ConcurrentQueue<IFieldable> CollectedFields
    {
      get
      {
        return this._fields;
      }
    }

    public LuceneDocumentBuilder(IIndexable indexable, IProviderUpdateContext context)
        : base(indexable, context)
    {
      this.Context = context;
      _fields = (base.GetType().BaseType.GetField("fields", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(this) as ConcurrentQueue<IFieldable>);
    }


    public override void AddField(string fieldName, object fieldValue, bool append = false)
    {
      var fieldMap = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName);
      var fieldNameUntranslated = fieldName;

      fieldName = this.Index.FieldNameTranslator.GetIndexFieldName(fieldName);

      var fieldSettings = this.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName) as LuceneSearchFieldConfiguration;

      if (fieldSettings != null)
      {
        if (fieldMap != null)
        {
          fieldValue = fieldMap.FormatForWriting(fieldValue);
        }

        this.AddField(fieldName, fieldValue, fieldSettings);
        return;
      }

      if (VerboseLogging.Enabled)
      {
        StringBuilder sb = new StringBuilder();
        sb.AppendFormat("Field: {0} (Adding field with no field configuration)" + Environment.NewLine, fieldName);
        sb.AppendFormat(" - value: {0}" + Environment.NewLine, fieldValue != null ? fieldValue.GetType().ToString() : "NULL");
        sb.AppendFormat(" - value: {0}" + Environment.NewLine, fieldValue);
        VerboseLogging.CrawlingLogDebug(sb.ToString);
      }

      object formattedValue;
      var multiValueField = fieldValue as IEnumerable;
      if (multiValueField != null && !(fieldValue is string))
      {
        foreach (var value in multiValueField)
        {
          formattedValue = this.Index.Configuration.IndexFieldStorageValueFormatter.FormatValueForIndexStorage(value, fieldNameUntranslated);
          if (fieldMap != null)
          {
            formattedValue = fieldMap.FormatForWriting(formattedValue);
          }

          if (formattedValue != null)
          {
            _fields.Enqueue(new Field(fieldName, formattedValue.ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
          }
        }

        return;
      }

      formattedValue = this.Index.Configuration.IndexFieldStorageValueFormatter.FormatValueForIndexStorage(fieldValue, fieldNameUntranslated);
      if (fieldMap != null)
      {
        formattedValue = fieldMap.FormatForWriting(formattedValue);
      }

      if (formattedValue != null)
      {
        _fields.Enqueue(new Field(fieldName, formattedValue.ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      }
    }

    public override void AddField(IIndexableDataField field)
    {
      var fieldMap = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(field);

      var value = this.Index.Configuration.FieldReaders.GetFieldValue(field);
      var name = field.Name;

      var fieldSettings = this.Index.Configuration.FieldMap.GetFieldConfiguration(field) as LuceneSearchFieldConfiguration;

      if (fieldSettings == null)
      {
        VerboseLogging.CrawlingLogDebug(() => string.Format("Cannot resolve field settings for field id:{0}, name:{1}, typeKey:{2} - The field will not be added to the index.", field.Id, field.Name, field.TypeKey));
        return;
      }

      value = fieldMap.FormatForWriting(value);

      var boost = BoostingManager.ResolveFieldBoosting(field);

      if (IndexOperationsHelper.IsTextField(field))
      {
        var contentSetting = this.Index.Configuration.FieldMap.GetFieldConfiguration(BuiltinFields.Content) as LuceneSearchFieldConfiguration;

        this.AddField(BuiltinFields.Content, value, contentSetting ?? this.defaultTextField);
      }

      this.AddField(name, value, fieldSettings, boost);
    }

    public override void AddBoost()
    {
      var itemBoost = BoostingManager.ResolveItemBoosting(this.Indexable);

      if (itemBoost > 0)
      {
        this.Document.Boost = itemBoost;
      }
    }

    protected new void AddField(string name, object value, LuceneSearchFieldConfiguration fieldSettings, float boost = 0)
    {
      Assert.IsNotNull(fieldSettings, "fieldSettings");

      name = this.Index.FieldNameTranslator.GetIndexFieldName(name);

      boost += fieldSettings.Boost;

      var multiValueField = value as IEnumerable;

      IFieldable field;

      if (multiValueField != null && !(value is string))
      {
        foreach (var innerVal in multiValueField)
        {
          var innerValTemp = fieldSettings.FormatForWriting(innerVal);

          field = LuceneFieldBuilder.CreateField(name, innerValTemp, fieldSettings, this.Index.Configuration.IndexFieldStorageValueFormatter);

          if (field != null)
          {
            field.Boost = boost;
            _fields.Enqueue(field);
          }
        }

        return;
      }

      value = fieldSettings.FormatForWriting(value);

      field = LuceneFieldBuilder.CreateField(name, value, fieldSettings, this.Index.Configuration.IndexFieldStorageValueFormatter);

      if (field != null)
      {
        field.Boost = boost;
        _fields.Enqueue(field);
      }
    }

    public override void AddComputedIndexFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddComputedIndexFields Start");
        if (this.IsParallelComputedFieldsProcessing)

        {
          this.AddComputedIndexFieldsInParallel();
        }
        else
        {
          this.AddComputedIndexFieldsInSequence();
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddComputedIndexFields End");
      }
    }

    protected new virtual void AddComputedIndexFieldsInParallel()
    {
      ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
      this.ParallelForeachProxy.ForEach(
          this.Options.ComputedIndexFields,
          this.ParallelOptions,
          (computedIndexField, parallelLoopState) =>
          {
            object fieldValue;

            try
            {
              fieldValue = computedIndexField.ComputeFieldValue(this.Indexable);
            }
            catch (Exception ex)
            {
              CrawlingLog.Log.Warn(string.Format("Could not compute value for ComputedIndexField: {0} for indexable: {1}", computedIndexField.FieldName, this.Indexable.UniqueId), ex);
              if (this.Settings.StopOnCrawlFieldError())
              {
                exceptions.Enqueue(ex);
                parallelLoopState.Stop();
              }

              System.Diagnostics.Debug.WriteLine(ex);
              return;
            }

            this.AddComputedIndexField(computedIndexField, fieldValue);
          });
      if (exceptions.Count > 0)
      {
        throw new AggregateException(exceptions);
      }
    }

    protected new virtual void AddComputedIndexFieldsInSequence()
    {
      foreach (var computedIndexField in this.Options.ComputedIndexFields)
      {
        object fieldValue;

        try
        {
          fieldValue = computedIndexField.ComputeFieldValue(this.Indexable);
        }
        catch (Exception ex)
        {
          CrawlingLog.Log.Warn(string.Format("Could not compute value for ComputedIndexField: {0} for indexable: {1}", computedIndexField.FieldName, this.Indexable.UniqueId), ex);
          if (this.Settings.StopOnCrawlFieldError())
          {
            throw;
          }

          System.Diagnostics.Debug.WriteLine(ex);
          continue;
        }

        this.AddComputedIndexField(computedIndexField, fieldValue);
      }
    }

    private void AddComputedIndexField(IComputedIndexField computedIndexField, object fieldValue)
    {
      var setting = this.Index.Configuration.FieldMap.GetFieldConfiguration(computedIndexField.FieldName) as LuceneSearchFieldConfiguration;

      if (fieldValue is IEnumerable && !(fieldValue is string))
      {
        foreach (var field in fieldValue as IEnumerable)
        {
          if (setting != null)
          {
            this.AddField(computedIndexField.FieldName, field, setting);
          }
          else
          {
            this.AddField(computedIndexField.FieldName, field);
          }
        }
      }
      else
      {
        if (setting != null)
        {
          this.AddField(computedIndexField.FieldName, fieldValue, setting);
        }
        else
        {
          this.AddField(computedIndexField.FieldName, fieldValue);
        }
      }
    }
  }
}