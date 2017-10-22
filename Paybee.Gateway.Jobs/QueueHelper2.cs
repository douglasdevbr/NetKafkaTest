using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Paybee.Gateway.Queue.Queue
{
    public class QueueHelper2
    {  
        public string Brokers
        {
            get;
            private set;
        }

        public Dictionary<string, object>  Config
        {
            get;
            private set;
        }

        public QueueHelper2()
        {
            Config = new Dictionary<string, object>();
            Config.Add("bootstrap.servers", "steamer-01.srvs.cloudkafka.com:9093,steamer-02.srvs.cloudkafka.com:9093,steamer-03.srvs.cloudkafka.com:9093");
            Config.Add("group.id", "teste");
            Config.Add("security.protocol", "SSL");
            Config.Add("ssl.certificate.location", "/Users/douglassilva/test/kafka.cert");
            Config.Add("ssl.key.location", "/Users/douglassilva/test/service.key");

            Config.Add("ssl.ca.location", "/Users/douglassilva/test/key.pem");

        //    Config.Add("default.topic.config", new Dictionary<string, object>
        //{
        //    { "auto.offset.reset", "smallest" }
        //});
     
        }

        public Consumer<Null, string> BasicComsumer(string topic)
        {
 
            Consumer<Null, string> consumer = new Consumer<Null, string>(Config, null, new StringDeserializer(Encoding.UTF8));
  
            return consumer;
        }

        public  void writeText( string text, string topic)
        {
             this.BasicProducer(text, topic);
        }

        private string BasicProducer(string text, string topic)
        {
            string result = string.Empty;
     

            using (var producer = new Producer<Null, string>(Config, null, new StringSerializer(Encoding.UTF8)))
            {
               // Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");
                int x = 1;
                for (x = 1; x <= 1000; x++)
                {
                    var deliveryReport = producer.ProduceAsync(topic, null, text + x.ToString());
                  // var result1 = deliveryReport.Result; 
                    Console.WriteLine("Writting Messages........" + x.ToString());
                   
                }
                  producer.Flush(TimeSpan.FromSeconds(5));

           
               // string text;
               
            }

          
            return result;
        }
    }
}
