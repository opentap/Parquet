using OpenTap;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ParquetResultListener
{
    [Display("ParquetResultListener")]
    public class ParquetResultListener : ResultListener
    {
        private Dictionary<Guid, TestPlanRun> _guidToPlanRuns = new Dictionary<Guid, TestPlanRun>();
        private Dictionary<Guid, TestStepRun> _guidToStepRuns = new Dictionary<Guid, TestStepRun>();
        private Dictionary<Schema, ParquetFile> _schemaToFiles = new Dictionary<Schema, ParquetFile>();

        public ParquetResultListener()
        {
            Name = nameof(ParquetResultListener);
        }

        public override void Open()
        {
            base.Open();
        }

        public override void Close()
        {
            base.Close();
        }

        public override void OnTestPlanRunStart(TestPlanRun planRun)
        {
            base.OnTestPlanRunStart(planRun);
            _guidToPlanRuns[planRun.Id] = planRun;
        }

        public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
        {
            base.OnTestPlanRunCompleted(planRun, logStream);
        }

        public override void OnTestStepRunStart(TestStepRun stepRun)
        {
            base.OnTestStepRunStart(stepRun);
            _guidToStepRuns[stepRun.Id] = stepRun;
        }

        public override void OnTestStepRunCompleted(TestStepRun stepRun)
        {
            base.OnTestStepRunCompleted(stepRun);
        }

        public override void OnResultPublished(Guid stepRunId, ResultTable result)
        {
            base.OnResultPublished(stepRunId, result);
            TestStepRun stepRun = _guidToStepRuns[stepRunId];
            TestPlanRun planRun = GetPlanRun(stepRun);

            Schema schema = CreateSchemaFromTable(result);
            if (!_schemaToFiles.TryGetValue(schema, out ParquetFile? file))
            {
                string dirName = $"Results/{planRun.TestPlanName}{planRun.StartTime.ToString("yy-MM-dd-HH-mm-ss")}";
                string fileName = $"{stepRun.TestStepTypeName}.parquet";
                if (!Directory.Exists(dirName))
                {
                    Directory.CreateDirectory(dirName);
                }
                file = new ParquetFile($"{dirName}/{fileName}", schema);
                _schemaToFiles.Add(schema, file);
            }
            file.PublishResult(result);
        }

        private Schema CreateSchemaFromTable(ResultTable result)
        {
            List<Field> fields = new List<Field>();

            foreach (ResultColumn column in result.Columns)
            {
                if (column.Data is not null && column.Data.Length != 0)
                {
                    fields.Add(new DataField(column.Name, column.Data.GetValue(0).GetType()));
                }
            }

            Schema schema = new Schema(fields);
            return schema;
        }

        private TestPlanRun GetPlanRun(TestStepRun run)
        {
            TestPlanRun? planRun;
            while (!_guidToPlanRuns.TryGetValue(run.Parent, out planRun))
            {
                run = _guidToStepRuns[run.Parent];
            }
            return planRun;
        }
    }
}