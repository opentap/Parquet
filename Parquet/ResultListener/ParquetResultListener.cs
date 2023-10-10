using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;

namespace OpenTap.Plugins.Parquet
{
    [Display("Parquet Result Listener", "Save results in a Parquet file", "Database")]
    public sealed class ParquetResultListener : ResultListener
    {
        private readonly Dictionary<Guid, string> _planGuidToDirectoryName = new Dictionary<Guid, string>();
        private readonly Dictionary<string, ParquetFile> _parquetFiles = new Dictionary<string, ParquetFile>();
        private readonly Dictionary<Guid, TestPlanRun> _guidToPlanRuns = new Dictionary<Guid, TestPlanRun>();
        private readonly Dictionary<Guid, TestStepRun> _guidToStepRuns = new Dictionary<Guid, TestStepRun>();
        private readonly HashSet<Guid> _hasWrittenParameters = new HashSet<Guid>();


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
            foreach (ParquetFile file in _parquetFiles.Values)
            {
                file.Dispose();
            }
            _parquetFiles.Clear();

            _planGuidToDirectoryName.Clear();
            _guidToPlanRuns.Clear();
            _guidToStepRuns.Clear();
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

            if (!_hasWrittenParameters.Contains(planRun.Id))
            {
                string path = $"{_planGuidToDirectoryName[planRun.Id]}{Path.DirectorySeparatorChar}{planRun.TestPlanName}.parquet";
                Schema schema = SchemaBuilder.FromTestPlanRun(planRun);
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
                string path = $"{_planGuidToDirectoryName[planRun.Id]}{Path.DirectorySeparatorChar}{stepRun.TestStepName}.parquet";
                Schema schema = SchemaBuilder.FromTestStepRun(stepRun);
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

            string path = $"{_planGuidToDirectoryName[planRun.Id]}{Path.DirectorySeparatorChar}{result.Name}.parquet";
            Schema schema = SchemaBuilder.FromResult(stepRun, result);
            ParquetFile file = GetOrCreateParquetFile(schema, path);
            file.Results(stepRun, result);

            _hasWrittenParameters.Add(stepRunId);
        }

        private ParquetFile GetOrCreateParquetFile(Schema schema, string suggestedPath)
        {
            string path = suggestedPath;
            int count = 0;
            while (true)
            {
                if (!_parquetFiles.TryGetValue(path, out ParquetFile? file))
                {
                    file = new ParquetFile(schema, path);
                    _parquetFiles[path] = file;
                    return file;
                }
                if (file.CanContain(schema))
                {
                    return file;
                }
                count += 1;
                string dirPath = new DirectoryInfo(suggestedPath).Parent.FullName;
                string fileName = $"{Path.GetFileNameWithoutExtension(suggestedPath)}({count}){Path.GetExtension(suggestedPath)}";
                path = Path.Combine(dirPath, fileName);
            }
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