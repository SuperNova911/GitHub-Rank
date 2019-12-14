using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace GitHubRankWeb.Models
{
    public class LicenseModel
    {
        public int Rank { get; set; }
        public string Name { get; set; }
        [DisplayFormat(DataFormatString = "{0:N0}")]
        public int Score { get; set; }
        [DisplayFormat(DataFormatString = "{0:N3}%")]
        public double Ratio { get; set; }
        public string Reference { get; set; }
    }
}
