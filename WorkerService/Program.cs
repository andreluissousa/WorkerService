using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing.Impl;
using WorkerService.Domain;

namespace WorkerService
{
    public class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                #region Consumindo fila

                var factory = new ConnectionFactory() {HostName = "localhost"};
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: 
                        "empresaQueue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    
                    channel.BasicQos(0, 1, false);
                    
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        try
                        {
                            var body = ea.Body;
                            var message = Encoding.UTF8.GetString(body);

                            var empresa = System.Text.Json.JsonSerializer.Deserialize<Empresa>(message);
                        
                            Console.WriteLine($"[x] Empresa com inscrição {empresa.Inscricao}|{empresa.NomeEmpresa}", message);
                            //throw new Exception(); //Descomentar caso deseje testar o cenário aonde tenha exceção na regra de negocio e verificar se a mensagem ainda está na fila
                            channel.BasicAck(ea.DeliveryTag, false); //Confirma que a operação foi concluída com sucesso e que a mensagem pode ser eliminada da fila
                        }
                        catch (Exception e)
                        {
                            channel.BasicNack(ea.DeliveryTag, false, true); //Caso ocorra alguma exceção a mensagem é retornada para a fila
                        }
                    };

                    channel.BasicConsume(
                        queue: "empresaQueue",
                        autoAck: false,//Deve estar false, pois, se falhar alguma mensagem, por meio do channel.BasicAck é possível fazer com que a mensagem volte para a fila
                        consumer: consumer);
                    
                    Console.WriteLine("Press [enter} to exit");
                    Console.ReadLine();
                }
                #endregion
            }
            catch (Exception ex)
            {
                
            } 
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) => { services.AddHostedService<Worker>(); });
    }
}