using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace GitHubRankWeb.Models
{
    public class RepositoryModel
    {
        public string Name { get; set; }
        public string OwnerName { get; set; }
        public string Description { get; set; }
        public string Language { get; set; }
        [DisplayFormat(DataFormatString = "{0:N0}")]
        public int Star { get; set; }
        [DisplayFormat(DataFormatString = "{0:N0}")]
        public int Fork { get; set; }
        public string HtmlUrl { get; set; }
        public string Reference { get; set; }
    }
}
