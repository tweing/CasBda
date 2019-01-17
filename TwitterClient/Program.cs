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
        const long minFileSizeLimit = 2 * 1024 * 1024; // 2 MB minimum otherwise the file name generation wouldn't work with Seconds in the name
        const long defaultFileSizeLimit = minFileSizeLimit;

        static void Main(string[] args)
        {
            var appSettings = ConfigurationManager.AppSettings;

            //Configure Twitter OAuth
            var oauthToken = appSettings["oauth_token"];
            var oauthTokenSecret = appSettings["oauth_token_secret"];
            var oauthCustomerKey = appSettings["oauth_consumer_key"];
            var oauthConsumerSecret = appSettings["oauth_consumer_secret"];
            var searchGroups = appSettings["twitter_keywords"];
            var removeAllUndefined = !string.IsNullOrWhiteSpace(appSettings["clear_all_with_undefined_sentiment"]) ?
                Convert.ToBoolean(appSettings["clear_all_with_undefined_sentiment"])
                : false;
            var sendExtendedInformation = !string.IsNullOrWhiteSpace(appSettings["send_extended_information"]) ?
                Convert.ToBoolean(appSettings["send_extended_information"])
                : false;
            var AzureOn = !string.IsNullOrWhiteSpace(appSettings["AzureOn"]) ?
                Convert.ToBoolean(appSettings["AzureOn"])
                : false;
            var mode = appSettings["match_mode"];
            var createBigFile = !string.IsNullOrWhiteSpace(appSettings["create_big_file"]) ?
                Convert.ToBoolean(appSettings["create_big_file"]) : false;

            long fileSizeLimit = !string.IsNullOrWhiteSpace(appSettings["filesizelimit"]) ?
                Convert.ToInt64(appSettings["filesizelimit"])
                : defaultFileSizeLimit;
            if (fileSizeLimit < minFileSizeLimit)
            {
                fileSizeLimit = minFileSizeLimit;
                Console.WriteLine("File size limit in config was too small and has been set to {0:N0}", fileSizeLimit);
            }

            var includeRetweets = !string.IsNullOrWhiteSpace(appSettings["IncludeRetweets"]) ?
                Convert.ToBoolean(appSettings["IncludeRetweets"])
                : false;

            //Configure EventHub
            var config = new EventHubConfig();
            config.ConnectionString = appSettings["EventHubConnectionString"];
            config.EventHubName = appSettings["EventHubName"];

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
