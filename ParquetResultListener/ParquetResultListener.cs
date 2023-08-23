using OpenTap;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ParquetResultListener
{
    [Display("ParquetResultListener")]
    public sealed class ParquetResultListener : ResultListener
    {
        private Dictionary<Guid, string> _planGuidToDirectoryName = new Dictionary<Guid, string>(); 
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
            _planGuidToDirectoryName.Clear();
            _guidToPlanRuns.Clear();
            _guidToStepRuns.Clear();
            _schemaToFiles.Clear();
        }

        public override void OnTestPlanRunStart(TestPlanRun planRun)
        {
            base.OnTestPlanRunStart(planRun);
            string dirName = $"Results/{planRun.TestPlanName}{planRun.StartTime.ToString("yy-MM-dd-HH-mm-ss")}";
            if (!Directory.Exists(dirName))
            {
                Directory.CreateDirectory(dirName);
            }
            _planGuidToDirectoryName[planRun.Id] = dirName;
            _guidToPlanRuns[planRun.Id] = planRun;
        }

        public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
        {
            base.OnTestPlanRunCompleted(planRun, logStream);

            if (_planGuidToDirectoryName.TryGetValue(planRun.Id, out var dirName))
            {
                foreach (var file in Directory.EnumerateFiles(dirName))
                {
                    planRun.PublishArtifact(file);
                }
            }
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

            Schema schema = CreateSchemaFromResult(stepRun, result);
            if (!_schemaToFiles.TryGetValue(schema, out ParquetFile? file))
            {
                string dirName = _planGuidToDirectoryName[planRun.Id];
                string fileName = $"{stepRun.TestStepName}.parquet";
                file = new ParquetFile($"{dirName}/{fileName}", schema);
                _schemaToFiles.Add(schema, file);
            }
            file.PublishResult(stepRun, result);
        }

        private Schema CreateSchemaFromResult(TestStepRun stepRun, ResultTable result)
        {
            SchemaBuilder schemaBuilder = new SchemaBuilder();

            schemaBuilder.AppendGuid("Guid");
            schemaBuilder.AppendGuid("Parent");

            foreach (ResultParameter parameter in stepRun.Parameters)
            {
                schemaBuilder.AppendStep(parameter.Value, parameter.Name);
            }

            foreach (ResultColumn column in result.Columns)
            {
                schemaBuilder.TryAppendResult(column.Data, column.Name);
            }

            return schemaBuilder.Build();
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