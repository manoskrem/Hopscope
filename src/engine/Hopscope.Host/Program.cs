using Hopscope.Infrastructure.Serialization;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default));

var app = builder.Build();

app.MapGet("/healthz", () => "OK");

app.Run();
