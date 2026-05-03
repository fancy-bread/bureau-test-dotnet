using BureauObservability.Web.Endpoints;
using BureauObservability.Web.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IEventStore, EventStore>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapEventsEndpoints();

app.Run();

public partial class Program { }
