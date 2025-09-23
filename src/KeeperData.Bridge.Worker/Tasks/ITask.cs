using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeeperData.Bridge.Worker.Tasks
{
    public interface ITask
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}
