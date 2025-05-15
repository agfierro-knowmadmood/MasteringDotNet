using Consumer;
using MasteringKafka.Shared;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection(KafkaSettings.Section));

var host = builder.Build();
host.Run();
