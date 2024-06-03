using MicroRabbit.Banking.Domain.Models;
using Microsoft.EntityFrameworkCore;

namespace MicroRabbit.Banking.Data.Context;

public class BankingDbContext : DbContext
{
    public BankingDbContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<Account> Accounts { get; set; }


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

        modelBuilder.Entity<Account>(x =>
        {
            x.Property(z => z.AccountBalance).HasColumnType("decimal(18,2)");
        });

        base.OnModelCreating(modelBuilder);
    }
}