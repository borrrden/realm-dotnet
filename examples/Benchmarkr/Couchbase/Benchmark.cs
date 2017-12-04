////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

using System;
using System.IO;
using System.Linq;
using Couchbase.Lite;
using System.Collections.Generic;

using Couchbase.Lite.Query;

namespace Benchmarkr.Couchbase
{
    public class Benchmark : BenchmarkBase
    {
        public override string Name
        {
            get
            {
                return "Couchbase";
            }
        }

        private Database db;
        public override IDisposable OpenDB()
        {
            var config = new DatabaseConfiguration
            {
                Directory = Path
            };

            return db = new Database("cbdb", config);
        }

        public override void DeleteDB()
        {
            try
            {
                Database.Delete("cbdb", Path);
            }
            catch (Exception)
            {
                
            }
        }

        public override void RunInTransaction(Action action)
        {
            db?.InBatch(() => { action(); });
        }

        public override void InsertObject(uint index)
        {
            using (var doc = new MutableDocument()) {
                doc.Set("name", NameValue(index))
                    .Set("age", AgeValue(index))
                    .Set("is_hired", IsHiredValue(index));
                db?.Save(doc);
            }
        }

        public override int Count(EmployeeQuery query)
        {
            using (var q = ConvertQuery(query))
                using(var result = q.Run()) {
                    return result.Count;
                }
        }

        public override long Enumerate(EmployeeQuery query)
        {
            using (var q = ConvertQuery(query))
            using(var result = q.Run()) {
                long ages = 0;
                foreach (var row in result)
                {
                    ages += row.GetLong("age");
                }
                return ages;
            }
        }

        private IQuery ConvertQuery(EmployeeQuery query)
        {
            return Query.Select(SelectResult.Expression(Expression.Property("age")))
                .From(DataSource.Database(db))
                .Where(Function.Contains(Expression.Property("name"), query.Name)
                    .And(Expression.Property("age").Between(query.MinAge, query.MaxAge))
                    .And(Expression.Property("is_hired").EqualTo(query.IsHired)));
        }
    }
}

