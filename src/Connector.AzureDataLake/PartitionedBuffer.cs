using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.OneLake
{
    internal class PartitionedBuffer<TPartition, TItem> : IDisposable
    {
        private readonly int _maxSize;
        private readonly int _timeout;
        private readonly Action<TPartition, TItem[]> _bulkAction;
        private readonly Dictionary<TPartition, Buffer<TItem>> _buffers;

        public PartitionedBuffer(int maxSize, int timeout, Action<TPartition, TItem[]> bulkAction)
        {
            _maxSize = maxSize;
            _timeout = timeout;
            _bulkAction = bulkAction;
            _buffers = new Dictionary<TPartition, Buffer<TItem>>();
        }

        public async Task Add(TPartition partition, TItem item)
        {
            Buffer<TItem> buffer;
            lock (_buffers)
            {
                if (!_buffers.TryGetValue(partition, out buffer))
                {
                    _buffers.Add(partition, buffer = new Buffer<TItem>(_maxSize, _timeout, x => _bulkAction(partition, x)));
                }
            }

            await buffer.Add(item);
        }

        public void Dispose()
        {
            foreach (var buffer in _buffers)
            {
                buffer.Value.Dispose();
            }
        }

        public async Task Flush()
        {
            foreach (var buffer in _buffers)
            {
                await buffer.Value.Flush();
            }
        }
    }
}
