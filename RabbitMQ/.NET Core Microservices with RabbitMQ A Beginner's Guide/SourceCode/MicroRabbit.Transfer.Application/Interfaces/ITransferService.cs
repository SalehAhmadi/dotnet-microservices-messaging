using MicroRabbit.Transfer.Domain.Models;

namespace MicroRabbit.Transfer.Application.Interfaces;

public interface ITransferService
{
    IEnumerable<TransferLog> GeTransferLogs();

}