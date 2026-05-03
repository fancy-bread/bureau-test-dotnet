using BureauObservability.Web.Models;

namespace BureauObservability.Web.Services;

public interface IEventStore
{
    void Add(BureauEvent bureauEvent);
    IReadOnlyList<BureauEvent> GetRecent(int count);
    KafkaConnectionState ConnectionState { get; }
    void UpdateConnectionState(KafkaConnectionState state);
}
