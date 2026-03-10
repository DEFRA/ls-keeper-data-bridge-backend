using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace KeeperData.SamAPI.Locations
{
    public class LocationData
    {
        public string Type { get; set; } = default!;
        public string Id { get; set; } = default!;
        public string? Name { get; set; }
        public Address Address { get; set; } = default!;
        public string? OsMapReference { get; set; }

        public List<LivestockUnit> LivestockUnits { get; set; } = [];
        public List<Facility> Facilities { get; set; } = [];

        public Dictionary<string, object>? Relationships { get; set; }
    }
}