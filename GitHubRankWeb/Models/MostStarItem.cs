using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GitHubRankWeb.Models
{
    public class MostStarItem
    {
        public int Rank { get; set; }
        public string AvatarUrl { get; set; }
        public string Name { get; set; }
        public int Star { get; set; }
        public string Reference { get; set; }
    }
}
