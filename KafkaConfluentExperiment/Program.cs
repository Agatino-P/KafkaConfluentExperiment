using Confluent.Kafka;
using KafkaConfluentExperiment;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Serilog;

internal class Program
{
    private static void Main(string[] args)
    {
        //Log.Logger = new LoggerConfiguration().MinimumLevel.Debug().WriteTo.Console().CreateLogger();

        IHost host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                services
                .AddOptions<KafkaOptions>()
                .BindConfiguration(typeof(KafkaOptions).Name)
                .ValidateDataAnnotations();


                services.AddSingleton<KafkaSeeker>();

                //services.AddHostedService<KafkaConsumer>();
                //services.AddHostedService<KafkaProducer>();

            })
            .UseSerilog((context, config) =>
            {
                config.Enrich.FromLogContext()
                .ReadFrom.Configuration(context.Configuration)
                    //.MinimumLevel.Debug()
                    .Enrich.WithMachineName()
                    .WriteTo.Console();
            })
            .Build();

        Task.Run(() => host.Run());

        KafkaSeeker kafkaSeeker = host.Services.GetRequiredService<KafkaSeeker>();

        RetryPolicy<string?> retryOnNullPolicy = Policy
            .HandleResult<string?>(r => r == null)
            .WaitAndRetry(3, retryAttempt =>
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt-1)) );

        //while (true)
        //{
            string? res = retryOnNullPolicy.Execute(() => kafkaSeeker.Seek("ASpecificKey"));
            if (res != null)
            {
                Console.WriteLine($"Found: {res}");
            }
            else
            {
                Console.WriteLine("Not Found");
            }

        //}



        Console.WriteLine("Hit Enter to stop");
        Console.ReadLine();


    }
}
