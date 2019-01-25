using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace MonboDbClientCore
{
    class Program
    {
        private const string EndpointUrl = "mongodb://localhost:27017";
        private const string DatabaseName = "CasBda";
        private const string DatabaseCollection = "Playground";
        private MongoClient client;

        static void Main(string[] args)
        {
            try
            {
                Program p = new Program();
                //p.UpdateDocs().Wait();
                //p.ImportTweetsFromFile().Wait();
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }

        }

        private async Task UpdateDocs()
        {
            Console.WriteLine("This Command will update the ObjectId of the whole database '{0}', collection '{1}' to the Tweet id_str!", 
                DatabaseName, DatabaseCollection);
            Console.WriteLine("Press 'Y' to continue");
            if (Console.ReadKey().Key != ConsoleKey.Y)
            {
                return;
            }

            this.client = new MongoClient(EndpointUrl);
            var db = this.client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<BsonDocument>(DatabaseCollection);

            using (IAsyncCursor<BsonDocument> cursor = await collection.FindAsync(new BsonDocument()))
            {
                while (await cursor.MoveNextAsync())
                {
                    IEnumerable<BsonDocument> batch = cursor.Current;
                    foreach (BsonDocument document in batch)
                    {
                        var filter = new FilterDefinitionBuilder<BsonDocument>().Eq("_id", document["_id"]);
                        document["_id"] = document["id_str"];
                        await collection.InsertOneAsync(document);
                        await collection.Find(filter).ForEachAsync(d => collection.DeleteOne(d));
                        Console.WriteLine("Document updated to id '{0}'", document["_id"]);
                    }
                }
            }
        }

        private async Task ImportTweetsFromFile()
        {
            string docPath = @"c:\Users\Tom\Documents\tweets\import";

            Console.WriteLine("This Command will import all *.json files in the path '{0}' into the database '{1}', collection '{2}'.!", 
                docPath, DatabaseName, DatabaseCollection);
            Console.WriteLine("Press 'Y' to continue");
            if (Console.ReadKey().Key != ConsoleKey.Y)
            {
                return;
            }

            this.client = new MongoClient(EndpointUrl);
            var db = this.client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<BsonDocument>(DatabaseCollection);


            var files = from file in Directory.EnumerateFiles(docPath, "*.json", SearchOption.AllDirectories)
                        from line in File.ReadLines(file)
                        select new
                        {
                            File = file,
                            Text = line
                        };

            foreach (var f in files)
            {
                string log = string.Format("{0}\t{1}", f.File, f.Text.Substring(0, 50));
                File.AppendAllText(docPath + @"\import.log", log);
                Console.WriteLine(log);
                var document = BsonSerializer.Deserialize<BsonDocument>(f.Text);
                await collection.InsertOneAsync(document);
            }
            Console.WriteLine("{ 0} files found.", files.Count().ToString());
        }

    }

}
