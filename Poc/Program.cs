namespace Poc
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Windows.Forms;
    using LiteDB;

    public static class Program
    {
        private const string DbName = "test.db";
        private const string LogName = "test-log.db";
        private const int Records = 100_0000;
        private const int Chunk = 1000;

        public static async Task Main(string[] args)
        {
            if (File.Exists(DbName))
            {
                File.Delete(DbName);
            }

            if (File.Exists(LogName))
            {
                File.Delete(LogName);
            }

            var context = new CustomContext();

            SynchronizationContext.SetSynchronizationContext(context);

            await Task.Factory.StartNew(
                () =>
                {
                    using (var db = new LiteDatabase(
                        new ConnectionString
                        {
                            Filename = DbName,
                        }))
                    {
                        var collection = db.GetCollection<TestRecord>();
                        collection.EnsureIndex(x => x.Index);

                        var data = GenerateData();

                        var sw = Stopwatch.StartNew();

                        foreach (var chunk in data)
                        {
                            collection.Upsert(chunk);
                        }

                        Console.Write(sw.Elapsed);
                    }
                },
                CancellationToken.None,
                TaskCreationOptions.None,
                TaskScheduler.FromCurrentSynchronizationContext());
        }

        private static IReadOnlyCollection<IReadOnlyCollection<TestRecord>> GenerateData()
        {
            var records = new List<TestRecord>(Records);

            for (var i = 0; i < Records; i++)
            {
                records.Add(new TestRecord
                {
                    Index = i,
                    First = Random.Shared.Next(),
                    Second = Random.Shared.Next(),
                });
            }

            return records.Chunk(Chunk).ToArray();
        }

        private sealed class TestRecord
        {
            public int Index { get; init; }

            public int First { get; init; }

            public int Second { get; init; }
        }

        private sealed class CustomContext : SynchronizationContext
        {
            public override void Post(SendOrPostCallback d, object? state)
            {
                Console.WriteLine("POST {0} {1}", d.Method, new StackTrace());
                base.Post(d, state);
            }

            public override void Send(SendOrPostCallback d, object? state)
            {
                Console.WriteLine("SEND {0} {1}", d.Method, new StackTrace());
                base.Send(d, state);
            }
        }
    }
}