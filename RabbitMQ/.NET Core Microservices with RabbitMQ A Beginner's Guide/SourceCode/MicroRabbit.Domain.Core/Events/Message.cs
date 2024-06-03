using MediatR;

namespace MicroRabbit.Domain.Core.Events;

public abstract class Message : IRequest<bool>
{
    protected Message()
    {
        MessageType = GetType().Name;
    }

    public string MessageType { get; protected set; }

}