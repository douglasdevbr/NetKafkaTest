using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Paybee.Gateway.Queue.Queue;

namespace Paybee.Gateway.Jobs
{
    class Program
    {
        static void Main(string[] args)
        {
            TestConsumer("4ybv-dado");

            TestWriteInBroker("4ybv-dado", "teste Mensagem");

        }


        private static void TestConsumer(string topic)
        {
            Dictionary<string, object> Config = new Dictionary<string, object>();
            Config.Add("bootstrap.servers", "steamer-01.srvs.cloudkafka.com:9093,steamer-02.srvs.cloudkafka.com:9093,steamer-03.srvs.cloudkafka.com:9093");
            Config.Add("group.id", "teste");
            Config.Add("security.protocol", "SSL");
            Config.Add("ssl.certificate.location", "/Users/douglassilva/test/kafka.cert");
            Config.Add("ssl.key.location", "/Users/douglassilva/test/service.key");
            Config.Add("ssl.ca.location", "/Users/douglassilva/test/key.pem");
            Config.Add("auto.commit.interval.ms", 500);
            Config.Add("default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    });

            Console.WriteLine("Receiving Messages........");
       
            Consumer<Null, string> consumer = new Consumer<Null, string>(Config, null, new StringDeserializer(Encoding.UTF8));
  

            consumer.OnMessage += (_, msg)
             => Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");

            var cancelled = false;
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cancelled = true;
            };

            consumer.Subscribe(new List<String> { "4ybv-dado" });

            while (!cancelled)
            {
                consumer.Poll(100);

            }

        }

        private static void TestWriteInBroker(string topic, string text)
        {
            Dictionary<string, object>  Config = new Dictionary<string, object>();
            Config.Add("bootstrap.servers", "steamer-01.srvs.cloudkafka.com:9093,steamer-02.srvs.cloudkafka.com:9093,steamer-03.srvs.cloudkafka.com:9093");
            Config.Add("group.id", "teste");
            Config.Add("security.protocol", "SSL");
            Config.Add("ssl.certificate.location", "/Users/douglassilva/test/kafka.cert");
            Config.Add("ssl.key.location", "/Users/douglassilva/test/service.key");

            Config.Add("ssl.ca.location", "/Users/douglassilva/test/key.pem");

            using (var producer = new Producer<Null, string>(Config, null, new StringSerializer(Encoding.UTF8)))
            {
                // Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");
                int x = 0;
                for (x = 1; x <= 1000; x++)
                {
                    var deliveryReport = producer.ProduceAsync(topic, null, text + x.ToString());
                    // var result1 = deliveryReport.Result; 
                   // Console.WriteLine("Writting Messages........" + x.ToString());

                }
                producer.Flush(TimeSpan.FromSeconds(5));


            }
        }
    }
}
