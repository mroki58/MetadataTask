using System.Collections.Concurrent;
using System.Threading.Tasks;

public class TtlDictionary<TKey, TValue> where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, (TValue Value, DateTime Expiry)> _dictionary = new();
    private readonly ConcurrentDictionary<TKey, SemaphoreSlim> _locks = new();

    public async Task<TValue> GetOrAdd(TKey key, Func<Task<TValue>> valueFactory, TimeSpan ttl)
    {
        if (_dictionary.TryGetValue(key, out var entry) && DateTime.UtcNow < entry.Expiry)
        {
            return entry.Value;
        }

        var myLock = _locks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));

        await myLock.WaitAsync();
        try
        {
            if (_dictionary.TryGetValue(key, out entry) && DateTime.UtcNow < entry.Expiry)
            {
                return entry.Value;
            }

            var newValue = await valueFactory();

            _dictionary[key] = (newValue, DateTime.UtcNow.Add(ttl));

            return newValue;
        }
        finally
        {
            myLock.Release();
        }
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        if (_dictionary.TryGetValue(key, out var entry) && DateTime.UtcNow < entry.Expiry)
        {
            value = entry.Value;
            return true;
        }

        value = default!;
        return false;
    }
}

