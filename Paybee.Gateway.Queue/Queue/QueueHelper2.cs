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
           //Config.Add("group.id", "teste");
            Config.Add("security.protocol", "SSL");
            Config.Add("ssl.certificate.location", "/Users/douglassilva/test/kafka.cert");
            Config.Add("ssl.key.location", "/Users/douglassilva/test/service.key");

            Config.Add("ssl.ca.location", "/Users/douglassilva/test/key.pem");
          //  Config.Add("ssl.key.password", "rx36me3lpfrfu2vp");

            //  Config = new KafkaOptions(new Uri("steamer-01.srvs.cloudkafka.com:9093"), new Uri("steamer-02.srvs.cloudkafka.com:9093"), new Uri("steamer-03.srvs.cloudkafka.com:9093"));

        }

        public  string writeText( string text, string topic)
        {
           return  this.BasicProducer(text, topic);
        }

        private string BasicProducer(string text, string topic)
        {
            string result = string.Empty;
            //var router = new BrokerRouter(Config);

            //var client = new Producer(router);
            //client.SendMessageAsync(topic, new[]{ new Message(text)}).Wait();

                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
               // producer.Flush(TimeSpan.FromSeconds(10));

            using (var producer = new Producer<Null, string>(Config, null, new StringSerializer(Encoding.UTF8)))
            {
               // Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");
                var deliveryReport = producer.ProduceAsync(topic, null, text);
                var result1 = deliveryReport.Result; 
           
               // string text;
               
            }

            //using (var consumer = new Consumer<Null, string>(Config, null, new StringDeserializer(Encoding.UTF8)))
            //{
            //    consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, 0) });

            //    while (true)
            //    {
            //        Message<Null, string> msg;
            //        if (consumer.Consume(out msg, TimeSpan.FromSeconds(1)))
            //        {
            //            result =  msg.Value;
            //            break;
            //        }
            //    }
            //}
            return result;
        }
    }
}
