using MicroRabbit.Transfer.Domain.Models;
using Microsoft.EntityFrameworkCore;

namespace MicroRabbit.Transfer.Data.Context;

public class TransferDbContext : DbContext
{
    public TransferDbContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<TransferLog> TransferLogs { get; set; }


    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        //General Solution for all decimals
        //var decimalProps = modelBuilder.Model
        //    .GetEntityTypes()
        //    .SelectMany(t => t.GetProperties())
        //    .Where(p => (System.Nullable.GetUnderlyingType(p.ClrType) ?? p.ClrType) == typeof(decimal));

        //foreach (var property in decimalProps)
        //{
        //    property.SetPrecision(18);
        //    property.SetScale(2);
        //}

        modelBuilder.Entity<TransferLog>(x =>
        {
            x.Property(z => z.TransferAmount).HasColumnType("decimal(18,2)");
        });

        base.OnModelCreating(modelBuilder);
    }
}