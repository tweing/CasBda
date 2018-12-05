//********************************************************* 
// 
//    Copyright (c) Microsoft. All rights reserved. 
//    This code is licensed under the Microsoft Public License. 
//    THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF 
//    ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING ANY 
//    IMPLIED WARRANTIES OF FITNESS FOR A PARTICULAR 
//    PURPOSE, MERCHANTABILITY, OR NON-INFRINGEMENT. 
// 
//*********************************************************


using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Security.Cryptography;
using Newtonsoft.Json;
using System.Text;
using System.Web;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;

namespace TwitterClient.Common
{
    public struct TwitterConfig
    {
        private const string TweetFileFormat = "tweet{0:yyyyMMddHHmmss}.json";

        public static string GetTweetFilename()
        {
            return string.Format(TweetFileFormat, DateTime.Now);
        }

        public readonly string OAuthToken;
        public readonly string OAuthTokenSecret;
        public readonly string OAuthConsumerKey;
        public readonly string OAuthConsumerSecret;
        public readonly string Keywords;
		public readonly string SearchGroups;
        public readonly bool CreateBigFile;
        public readonly string FolderName;
        public readonly string BigFileName;
        public readonly bool IncludeRetweets;

        public TwitterConfig(string oauthToken, string oauthTokenSecret, string oauthConsumerKey, string oauthConsumerSecret, 
            string keywords, string searchGroups, bool createBigFile, string folderName, string bigFileName, bool includeRetweets)
        {
            OAuthToken = oauthToken;
            OAuthTokenSecret = oauthTokenSecret;
            OAuthConsumerKey = oauthConsumerKey;
            OAuthConsumerSecret = oauthConsumerSecret;
            Keywords = keywords;
			SearchGroups = searchGroups;
            CreateBigFile = createBigFile;
            FolderName = folderName;
            BigFileName = bigFileName;
            IncludeRetweets = includeRetweets;

        }
    }

    [DataContract]
    public class TwitterUser
    {
        [DataMember(Name = "time_zone")]               public string TimeZone;
        [DataMember(Name = "name")]                    public string Name;
        [DataMember(Name = "profile_image_url")]       public string ProfileImageUrl;
    }

    [DataContract]
    public class TwitterPlace
    {
        [DataMember(Name = "id")]                       public string id;
        [DataMember(Name = "url")]                      public string Url;
        [DataMember(Name = "place_type")]               public string PlaceType;
        [DataMember(Name = "name")]                     public string Name;
        [DataMember(Name = "full_name")]                public string FullName;
        [DataMember(Name = "country_code")]             public string CountryCode;
        [DataMember(Name = "bounding_box")]             public TwitterBoundingBox BoundingBox;
        [DataMember(Name = "attributes")]               public TwitterAttributes Attributes;
    }

    [DataContract]
    public class RetweetedStatus
    {
        [DataMember(Name = "created_at")]               public string CreatedAt;
        [DataMember(Name = "id")]                       public Int64 Id;
    }

    [DataContract]
    public class TwitterAttributes
    {
    }

    [DataContract]
    public class TwitterBoundingBox
    {
    }

    [DataContract]
    public class Tweet
    {
        [DataMember(Name = "id")]                      public Int64 Id;
        [DataMember(Name = "in_reply_to_status_id")]   public Int64? ReplyToStatusId;
        [DataMember(Name = "in_reply_to_user_id")]     public Int64? ReplyToUserId;
        [DataMember(Name = "in_reply_to_screen_name")] public string ReplyToScreenName;
        [DataMember(Name = "retweeted")]               public bool Retweeted;
        [DataMember(Name = "text")]                    public string Text;
        [DataMember(Name = "lang")]                    public string Language;
        [DataMember(Name = "source")]                  public string Source;
        [DataMember(Name = "retweet_count")]           public string RetweetCount;
        [DataMember(Name = "user")]                    public TwitterUser User;
        [DataMember(Name = "created_at")]              public string CreatedAt;
        [DataMember(Name = "place")]                   public TwitterPlace Place;
        [DataMember(Name = "retweeted_status")]        public RetweetedStatus RTStatus;
        [IgnoreDataMember]                             public string RawJson;


        private int numberTweets;

        public Tweet()
		{
			keepRunning = true;
		}
		public bool keepRunning { get; set; }
        public IEnumerable<Tweet> StreamStatuses(TwitterConfig config)
        {
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(typeof(Tweet));

            var streamReader = ReadTweets(config);

            while (keepRunning)
            {
                string line = null;
                try { line = streamReader.ReadLine(); }
                catch (Exception) { }

                if (!string.IsNullOrWhiteSpace(line) && !line.StartsWith("{\"delete\"") && !line.StartsWith("{\"limit\""))
                {
                    Debug.WriteLine(line);
                    // Sometimes the line is not correctly read which will end up in a serialization exception
                    dynamic result = null;
                    try
                    {
                        result = (Tweet)jsonSerializer.ReadObject(new MemoryStream(Encoding.UTF8.GetBytes(line)));
                        result.RawJson = line;
                        numberTweets++;

                        if (result.CreatedAt == null)
                        {
                            Console.Write("potential limit");
                        }
                        else
                        {
                            if ((config.IncludeRetweets == false) && IsRetweet(result))
                            {
                                var previousColor = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.DarkYellow;
                                Console.WriteLine("{0} Retweet will not be processed ****", numberTweets);
                                Console.ForegroundColor = previousColor;
                            }
                            else
                            {
                                WriteToFile(result, config.CreateBigFile, config.FolderName, config.BigFileName);
                                WriteToConsole(result);
                            }
                        }
                    }
                    catch (SerializationException ex1)
                    {
                        WriteException(line, ex1);
                    }
                    catch (JsonSerializationException ex2)
                    {
                        WriteException(line, ex2);
                    }

                    if (result != null)
                    {
                        yield return result;
                    }
                }

                // Oops the Twitter has ended... or more likely some error have occurred.
                // Reconnect to the twitter feed.
                if (line == null)
                {
                    Console.BackgroundColor = ConsoleColor.Red;
                    Console.WriteLine("Potential Limit reached... will reconnect to feed in an instant.");
                    Console.BackgroundColor = ConsoleColor.Black;

                    // As an exception we use Thread.Sleep here
                    Thread.Sleep(1000);

                    streamReader = ReadTweets(config);
                }
            }
        }

        private static void WriteException(string line, Exception e)
        {
            var previousColor = Console.BackgroundColor;
            Console.BackgroundColor = ConsoleColor.Red;
            Console.WriteLine("Line could not be de-serialized: " + line);
            Console.BackgroundColor = previousColor;
        }

        private bool IsRetweet(Tweet result)
        {
            // According to https://stackoverflow.com/questions/29689566/exclude-retweets-from-twitter-streaming-api-using-tweepy
            // retweets are officially marked with the Retweeted attribute set to true, but sometimes people retweet with adding RT
            // in front of the text. The post recommends if we want to exclude 'unofficial' retweets to include searching for 'RT @':
            //   if not tweet['retweeted'] and 'RT @' not in tweet['text']:

            // According to https://www.dataquest.io/blog/streaming-data-python/ 
            // retweets are detected by the retweeted_status: "We can filter out retweets by checking for the retweeted_status property."
            if (result.RTStatus != null)
            {
                return true;
            }
            return false;
        }

        private void WriteToConsole(Tweet tweet)
        {
            var serialisedString = JsonConvert.SerializeObject(tweet);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("{0} Tweet to Disk at: {1} : {2}", numberTweets, tweet.CreatedAt.ToString(), serialisedString);
        }

        private void WriteToFile(Tweet tweet, bool createBigFile, string folderName, string bigFileName)
        {
            // need to deserialize and re-serialize to get rid of \u escaped text
            dynamic jsonObject = JsonConvert.DeserializeObject(tweet.RawJson);
            var rawJson = JsonConvert.SerializeObject(jsonObject);

            // Add a new line to the end of the raw JSON for easier separation in the file:
            rawJson += Environment.NewLine;

            if (createBigFile)
            {
                string path = Path.Combine(folderName, bigFileName);
                File.AppendAllText(path, rawJson, Encoding.UTF8);
            }
            else
            {
                string path = Path.Combine(folderName, TwitterConfig.GetTweetFilename());
                File.WriteAllText(path, rawJson, Encoding.UTF8);
            }
        }

        public HttpWebRequest Request { get;  set;}

		static TextReader ReadTweets(TwitterConfig config)
        {
            const string TwitterBaseUrlStreamingTweets = "https://stream.twitter.com/1.1/statuses/filter.json";

            string  authHeader = CreateOAuthHeader(config, TwitterBaseUrlStreamingTweets);

            // make the request
            ServicePointManager.Expect100Continue = false;

            var postBody = "track=" + HttpUtility.UrlEncode(config.Keywords);
            string resource_url = TwitterBaseUrlStreamingTweets + "?" + postBody;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(resource_url);
            request.Headers.Add("Authorization", authHeader);
            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            request.PreAuthenticate = true;
            request.AllowWriteStreamBuffering = true;
            request.CachePolicy = new System.Net.Cache.RequestCachePolicy(System.Net.Cache.RequestCacheLevel.BypassCache);

            // bail out and retry after 5 seconds
            var responseTask = request.GetResponseAsync();
            if (responseTask.Wait(5000))
                return new StreamReader(responseTask.Result.GetResponseStream(), Encoding.UTF8); // TWE: added Encoding.UTF8
            else
            {
                request.Abort();
                return StreamReader.Null;
            }
        }

        private static string CreateOAuthHeader(TwitterConfig config, string baseUrl)
        {
            var oauth_version = "1.0";
            var oauth_signature_method = "HMAC-SHA1";

            // unique request details
            var oauth_nonce = Convert.ToBase64String(new ASCIIEncoding().GetBytes(DateTime.Now.Ticks.ToString()));
            var oauth_timestamp = Convert.ToInt64(
                (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc))
                    .TotalSeconds).ToString();

            // create oauth signature
            var baseString = string.Format(
                "oauth_consumer_key={0}&oauth_nonce={1}&oauth_signature_method={2}&" +
                "oauth_timestamp={3}&oauth_token={4}&oauth_version={5}&track={6}",
                config.OAuthConsumerKey,
                oauth_nonce,
                oauth_signature_method,
                oauth_timestamp,
                config.OAuthToken,
                oauth_version,
                Uri.EscapeDataString(config.Keywords));

            baseString = string.Concat("POST&", Uri.EscapeDataString(baseUrl), "&", Uri.EscapeDataString(baseString));

            var compositeKey = string.Concat(Uri.EscapeDataString(config.OAuthConsumerSecret),
            "&", Uri.EscapeDataString(config.OAuthTokenSecret));

            string oauth_signature;
            using (var hasher = new HMACSHA1(Encoding.ASCII.GetBytes(compositeKey)))
            {
                oauth_signature = Convert.ToBase64String(
                hasher.ComputeHash(Encoding.ASCII.GetBytes(baseString)));
            }

            // create the request header
            return string.Format(
                "OAuth oauth_nonce=\"{0}\", oauth_signature_method=\"{1}\", " +
                "oauth_timestamp=\"{2}\", oauth_consumer_key=\"{3}\", " +
                "oauth_token=\"{4}\", oauth_signature=\"{5}\", " +
                "oauth_version=\"{6}\"",
                Uri.EscapeDataString(oauth_nonce),
                Uri.EscapeDataString(oauth_signature_method),
                Uri.EscapeDataString(oauth_timestamp),
                Uri.EscapeDataString(config.OAuthConsumerKey),
                Uri.EscapeDataString(config.OAuthToken),
                Uri.EscapeDataString(oauth_signature),
                Uri.EscapeDataString(oauth_version)
            );
        }
    }


    public class TwitterPayload
    {
        public Int64 ID;
        public DateTime CreatedAt;
        public string UserName;
        public string TimeZone;
        public string ProfileImageUrl;
        public string Text;
        public string Language;
        public string Topic;
        public int SentimentScore;

        public string RawJson;

        public override string ToString()
        {
            return new { ID, CreatedAt, UserName, TimeZone, ProfileImageUrl, Text, Language, Topic, SentimentScore }.ToString();
        }
    }

    public class Payload
    {
        public DateTime CreatedAt { get; set; }
        public string Topic { get; set; }
		public int SentimentScore { get; set; }
		public string Author { get; set; }
		public string Text { get; set; }
		public bool SendExtended { get; set; }
        public string Language { get; set; }
		public override string ToString()
        {
            return SendExtended ?  new { CreatedAt, Topic, SentimentScore, Author, Text, Language }.ToString() : new { CreatedAt, Topic, SentimentScore }.ToString();
        }
    }
}
