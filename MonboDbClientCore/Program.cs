using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
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
        private const string DatabaseCollection = "Tweets";
        private MongoClient client;

        public class Tweet
        {
            [BsonId]
            public long id { get; set; }

            [DataMember(Name = "in_reply_to_status_id")] public Int64? ReplyToStatusId;
            [DataMember(Name = "in_reply_to_user_id")] public Int64? ReplyToUserId;
            [DataMember(Name = "in_reply_to_screen_name")] public string ReplyToScreenName;
            [DataMember(Name = "retweeted")] public bool Retweeted;
            [DataMember(Name = "text")] public string Text;
            [DataMember(Name = "lang")] public string Language;
            [DataMember(Name = "source")] public string Source;
            [DataMember(Name = "retweet_count")] public string RetweetCount;
            [DataMember(Name = "user")] public TwitterUser User;
            [DataMember(Name = "created_at")] public string CreatedAt;
            [DataMember(Name = "place")] public TwitterPlace Place;
            [DataMember(Name = "retweeted_status")] public RetweetedStatus RTStatus;

            //[BsonElement("description")]
            //public string Description { get; set; }
        }

        public class TwitterUser
        {
            [DataMember(Name = "time_zone")] public string TimeZone;
            [DataMember(Name = "name")] public string Name;
            [DataMember(Name = "profile_image_url")] public string ProfileImageUrl;
        }

        public class TwitterPlace
        {
            [DataMember(Name = "id")] public string id;
            [DataMember(Name = "url")] public string Url;
            [DataMember(Name = "place_type")] public string PlaceType;
            [DataMember(Name = "name")] public string Name;
            [DataMember(Name = "full_name")] public string FullName;
            [DataMember(Name = "country_code")] public string CountryCode;
            [DataMember(Name = "bounding_box")] public TwitterBoundingBox BoundingBox;
            [DataMember(Name = "attributes")] public TwitterAttributes Attributes;
        }

        public class RetweetedStatus
        {
            [DataMember(Name = "created_at")] public string CreatedAt;
            [DataMember(Name = "id")] public Int64 Id;
        }

        public class TwitterAttributes
        {
        }

        public class TwitterBoundingBox
        {
        }

        static void Main(string[] args)
        {
            try
            {
                Program p = new Program();
                p.GetStartedDemo().Wait();
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

        private async Task GetStartedDemo()
        {
            this.client = new MongoClient(EndpointUrl);
            var db = this.client.GetDatabase(DatabaseName);
            var collection = db.GetCollection<BsonDocument>(DatabaseCollection);

            string docPath = @"c:\Users\Tom\Documents\tweets\";
                // Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

            var files = from file in Directory.EnumerateFiles(docPath, "*.json", SearchOption.AllDirectories)
                        from line in File.ReadLines(file)
                        select new
                        {
                            File = file,
                            Text = line
                        };

            foreach (var f in files)
            { 
                Console.WriteLine("{0}\t{1}", f.File, f.Text.Substring(0, 50));
                // Tweet tweet = JsonConvert.DeserializeObject<Tweet>(line.Text);
                var document = BsonSerializer.Deserialize<BsonDocument>(f.Text);
                await InsertRecord(collection, document);
            }
            Console.WriteLine("{0} files found.", files.Count().ToString());
        }

        private async Task InsertRecord(IMongoCollection<BsonDocument> collection, BsonDocument tweet)
        {
            await collection.InsertOneAsync(tweet);
        }

        private async Task InsertRecord(IMongoCollection<Tweet> collection, Tweet tweet)
        {
            await collection.InsertOneAsync(tweet);
        }


        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

    }

}
