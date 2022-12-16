using KafkaConfluentExperiment;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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

                //services.AddSingleton(Log.Logger);

                services.AddHostedService<KafkaConsumer>();
                services.AddHostedService<KafkaProducer>();

            })
            .UseSerilog((context, config)=>{
                config.Enrich.FromLogContext()
                .ReadFrom.Configuration(context.Configuration)
                    //.MinimumLevel.Debug()
                    .Enrich.WithMachineName()
                    .WriteTo.Console(); 
            })
            .Build();

        Task.Run(() => host.Run());

        Console.WriteLine("Hit Enter to stop");
        Console.ReadLine();


    }
}