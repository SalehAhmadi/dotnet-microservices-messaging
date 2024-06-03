
using Microsoft.OpenApi.Models;
using System.Reflection;
using MicroRabbit.Domain.Core.Bus;
using MicroRabbit.Infra.IoC;
using MicroRabbit.Transfer.Data.Context;
using MicroRabbit.Transfer.Domain.EventHandlers;
using MicroRabbit.Transfer.Domain.Events;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.EntityFrameworkCore;

namespace MicroRabbit.Transfer.Api
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();

            builder.Services.AddDbContext<TransferDbContext>(options =>
            {
                options.UseSqlServer(builder.Configuration.GetConnectionString("TransferDbConnection"));
            });

            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen(x =>
            {
                x.SwaggerDoc("v1", new OpenApiInfo() { Title = "Transfer MicroService", Version = "v1" });
            });

            builder.Services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(Assembly.GetExecutingAssembly()));

            RegisterServices(builder.Services);

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI(x =>
                {
                    x.SwaggerEndpoint("/swagger/v1/swagger.json", "Transfer MicroService V1");
                });
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();


            app.MapControllers();

            ConfigureEventBus(app);

            app.Run();
        }

        private static void ConfigureEventBus(IApplicationBuilder app)
        {
            var eventBus = app.ApplicationServices.GetRequiredService<IEventBus>();
            eventBus.Subscribe<TransferCreatedEvent, TransferEventHandler>();
        }

        private static void RegisterServices(IServiceCollection builderServices)
        {
            DependencyContainer.RegisterServices(builderServices);
        }
    }
}