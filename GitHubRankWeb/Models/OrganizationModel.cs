using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace GitHubRankWeb.Models
{
    public class OrganizationModel
    {
        public string AvatarUrl { get; set; }
        public string Name { get; set; }
        [DisplayFormat(DataFormatString = "{0:N0}")]
        public int Star { get; set; }
        [DisplayFormat(DataFormatString = "{0:N0}")]
        public int Fork { get; set; }
        public string Email { get; set; }
        public string Location { get; set; }
        public string HtmlUrl { get; set; }
        public string BlogUrl { get; set; }
        public string Reference { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime FetchedAt { get; set; }
        public List<RepositoryModel> RepositoryModels { get; set; }
    }
}
