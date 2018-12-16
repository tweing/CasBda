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
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using TwitterClient.Common;
using System.IO;
using System.Diagnostics;

namespace TwitterClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var appsettings = ConfigurationManager.AppSettings;

            //Configure Twitter OAuth
            var oauthToken = ConfigurationManager.AppSettings["oauth_token"];
            var oauthTokenSecret = ConfigurationManager.AppSettings["oauth_token_secret"];
            var oauthCustomerKey = ConfigurationManager.AppSettings["oauth_consumer_key"];
            var oauthConsumerSecret = ConfigurationManager.AppSettings["oauth_consumer_secret"];
            var searchGroups = ConfigurationManager.AppSettings["twitter_keywords"];
            var removeAllUndefined = !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["clear_all_with_undefined_sentiment"]) ?
                Convert.ToBoolean(ConfigurationManager.AppSettings["clear_all_with_undefined_sentiment"])
                : false;
            var sendExtendedInformation = !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["send_extended_information"]) ?
                Convert.ToBoolean(ConfigurationManager.AppSettings["send_extended_information"])
                : false;
            var AzureOn = !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["AzureOn"]) ?
                Convert.ToBoolean(ConfigurationManager.AppSettings["AzureOn"])
                : false;
            var mode = ConfigurationManager.AppSettings["match_mode"];
            var createBigFile = !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["create_big_file"]) ?
                Convert.ToBoolean(ConfigurationManager.AppSettings["create_big_file"]) : false;
            long fileSizeLimit = !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["filesizelimit"]) ? 
                Convert.ToInt64(ConfigurationManager.AppSettings["filesizelimit"]) 
                : 524288000;
            var includeRetweets = !string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["IncludeRetweets"]) ?
                Convert.ToBoolean(ConfigurationManager.AppSettings["IncludeRetweets"])
                : false;

            //Configure EventHub
            var config = new EventHubConfig();
            config.ConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];
            config.EventHubName = ConfigurationManager.AppSettings["EventHubName"];

            var myEventHubObserver = new EventHubObserver(config, AzureOn);
            var keywords = searchGroups.Contains('|') ? string.Join(",", searchGroups.Split('|')) : searchGroups;
            var tweet = new Tweet();
            Console.WriteLine("Searching Tweets for keywords: {0}", keywords);

            var folderName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), @"tweets\");
            EnsureDirectory(folderName);

            var twitterConfig = new TwitterConfig(oauthToken, oauthTokenSecret, oauthCustomerKey, oauthConsumerSecret, 
                keywords, searchGroups, createBigFile, folderName, includeRetweets, fileSizeLimit);

            // Write out the config especially the keywords... (can only happen AFTER TwitterConfig has been created!
            string path = Path.Combine(folderName, Path.ChangeExtension(twitterConfig.BigFileName, ".config"));
            File.WriteAllText(path, @"CAS BDA Search (Team Pharma) for Tweets was started with the following keywords:" + Environment.NewLine + Environment.NewLine + keywords, Encoding.UTF8);

            // test
            foreach (var sendingPayload in tweet.StreamStatuses(twitterConfig))
            { }

            // end test
            //**var sendingPayload = tweet.StreamStatuses(twitterConfig).Where(e => !string.IsNullOrWhiteSpace(e.Text)).Select(t => Sentiment.ComputeScore(t, searchGroups, mode)).Select(t => new Payload { CreatedAt = t.CreatedAt, Topic = t.Topic, SentimentScore = t.SentimentScore, Author = t.UserName, Text = t.Text, SendExtended = sendExtendedInformation, Language = t.Language});
            //if (removeAllUndefined)
            //{
            //	sendingPayload = sendingPayload.Where(e => e.SentimentScore > -1);
            //}
            //sendingPayload.Where(e => e.Topic != "No Match").ToObservable().Subscribe(myEventHubObserver);
        }

        private static void EnsureDirectory(string folderName)
        {
            if (Directory.Exists(folderName) == false)
            {
                Directory.CreateDirectory(folderName);
            }
        }
    }
}
