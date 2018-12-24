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
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Configuration;

namespace TwitterClientCore
{
    public class EventHubObserver : IObserver<Payload>
    {
        private EventHubConfig _config;
        // private EventHubClient _eventHubClient;
        public bool AzureOn { get; set; }
                
        public EventHubObserver(EventHubConfig config, bool azureOn = true)
        {
			AzureOn = azureOn;
            try
            {
				
                _config = config;
				if (AzureOn)
				{
					// _eventHubClient = EventHubClient.CreateFromConnectionString(_config.ConnectionString, config.EventHubName);
				}
            }
            catch (Exception ex)
            {
               
            }

        }
        public void OnNext(Payload TwitterPayloadData)
        {
            return;
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Exception occured: " + error.ToString());
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
            Environment.Exit(1);
        }

    }
}
