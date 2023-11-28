using Parquet.Data;
using Parquet.Data.Rows;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;

namespace OpenTap.Plugins.Parquet
{
    [Display("Parquet", "Save results in a Parquet file", "Database")]
    public sealed class ParquetResultListener : ResultListener
    {
        private readonly Dictionary<string, ParquetFile> _parquetFiles = new Dictionary<string, ParquetFile>();
        private readonly Dictionary<Guid, TestPlanRun> _guidToPlanRuns = new Dictionary<Guid, TestPlanRun>();
        private readonly Dictionary<Guid, TestStepRun> _guidToStepRuns = new Dictionary<Guid, TestStepRun>();
        private readonly HashSet<Guid> _hasWrittenParameters = new HashSet<Guid>();

        [Display("File path", "The file path of the parquet file(s). Can use <ResultType> to have one file per result type.")]
        [FilePath]
        public MacroString FilePath { get; set; } = new MacroString() { Text = "Results/<TestPlanName>.<Date>/<ResultType>.parquet" };

        public ParquetResultListener()
        {
            Name = "Parquet";
        }

        public override void Open()
        {
            base.Open();
        }

        public override void Close()
        {
            base.Close();
            foreach (ParquetFile file in _parquetFiles.Values)
            {
                file.Dispose();
            }
            _parquetFiles.Clear();

            _guidToPlanRuns.Clear();
            _guidToStepRuns.Clear();
        }

        public override void OnTestPlanRunStart(TestPlanRun planRun)
        {
            base.OnTestPlanRunStart(planRun);

            //string dirName = $"Results/{planRun.TestPlanName}{planRun.StartTime.ToString("yy-MM-dd-HH-mm-ss")}";
            //if (!Directory.Exists(dirName))
            //{
            //    Directory.CreateDirectory(dirName);
            //}

            _guidToPlanRuns[planRun.Id] = planRun;
        }

        public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
        {
            base.OnTestPlanRunCompleted(planRun, logStream);

            if (!_hasWrittenParameters.Contains(planRun.Id))
            {
                string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
                {
                    { "ResultType", "Plan" }
                });
                SchemaBuilder schema = new SchemaBuilder();
                schema.AddPlanParameters(planRun);
                ParquetFile file = GetOrCreateParquetFile(schema, path);
                file.OnlyParameters(planRun);
                _hasWrittenParameters.Add(planRun.Id);
            }

            foreach (ParquetFile file in _parquetFiles.Values)
            {
                file.Dispose();
                planRun.PublishArtifact(file.Path);
            }
            _parquetFiles.Clear();
        }

        public override void OnTestStepRunStart(TestStepRun stepRun)
        {
            base.OnTestStepRunStart(stepRun);
            _guidToStepRuns[stepRun.Id] = stepRun;
        }

        public override void OnTestStepRunCompleted(TestStepRun stepRun)
        {
            base.OnTestStepRunCompleted(stepRun);

            if (!_hasWrittenParameters.Contains(stepRun.Id))
            {
                TestPlanRun planRun = GetPlanRun(stepRun);
                string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
                {
                    { "ResultType", "Plan" }
                });
                SchemaBuilder schema = new SchemaBuilder();
                schema.AddStepParameters(stepRun);
                ParquetFile file = GetOrCreateParquetFile(schema, path);
                file.OnlyParameters(stepRun);
                _hasWrittenParameters.Add(stepRun.Id);
            }
        }

        public override void OnResultPublished(Guid stepRunId, ResultTable result)
        {
            base.OnResultPublished(stepRunId, result);
            TestStepRun stepRun = _guidToStepRuns[stepRunId];
            TestPlanRun planRun = GetPlanRun(stepRun);

            string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
            {
                { "ResultType", result.Name }
            });
            SchemaBuilder schema = new SchemaBuilder();
            schema.AddResultFields(stepRun, result);
            ParquetFile file = GetOrCreateParquetFile(schema, path);
            file.Results(stepRun, result);

            _hasWrittenParameters.Add(stepRunId);
        }

        private ParquetFile GetOrCreateParquetFile(SchemaBuilder schema, string path)
        {
            if (!_parquetFiles.TryGetValue(path, out ParquetFile? file))
            {
                string dirPath = Path.GetDirectoryName(path);
                if (!Directory.Exists(dirPath))
                {
                    Directory.CreateDirectory(dirPath);
                }

                file = new ParquetFile(schema, path);
                _parquetFiles[path] = file;
            }
            else if (!file.CanContain(schema))
            {
                file.Dispose();
                file = new ParquetFile(schema, path);
                _parquetFiles[path] = file;
            }
            return file;
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