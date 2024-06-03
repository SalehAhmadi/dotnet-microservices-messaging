
using MicroRabbit.Banking.Data.Context;
using MicroRabbit.Infra.IoC;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
using System.Reflection;

namespace MicroRabbit.Banking.API
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();

            builder.Services.AddDbContext<BankingDbContext>(options =>
            {
                options.UseSqlServer(builder.Configuration.GetConnectionString("BankingDbConnection"));
            });
            
            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen(x =>
            {
                x.SwaggerDoc("v1", new OpenApiInfo() { Title = "Banking MicroService", Version = "v1" });
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
                    x.SwaggerEndpoint("/swagger/v1/swagger.json", "Banking MicroService V1");
                });
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();


            app.MapControllers();

            app.Run();
        }

        private static void RegisterServices(IServiceCollection builderServices)
        {
            DependencyContainer.RegisterServices(builderServices);
        }
    }
}