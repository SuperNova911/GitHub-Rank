using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace GitHubDataCollector
{
    public sealed class License : IEquatable<License>
    {
        public string Key { get; private set; }
        public string NodeId { get; private set; }
        public string Name { get; private set; }
        public string SpdxId { get; private set; }
        public string ApiUrl { get; private set; }

        public License(Octokit.LicenseMetadata licenseMetadata)
        {
            Key = licenseMetadata.Key;
            NodeId = licenseMetadata.NodeId;
            Name = licenseMetadata.Name;
            SpdxId = licenseMetadata.SpdxId ?? string.Empty;
            ApiUrl = licenseMetadata.Url != null ? licenseMetadata.Url : string.Empty;
        }

        public License(string key, string nodeId, string name, string spdxId, string apiUrl)
        {
            Key = key ?? throw new ArgumentNullException(nameof(key));
            NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            SpdxId = spdxId ?? throw new ArgumentNullException(nameof(spdxId));
            ApiUrl = apiUrl ?? string.Empty;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as License);
        }

        public bool Equals([AllowNull] License other)
        {
            return other != null &&
                   Key == other.Key;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Key);
        }

        public static bool operator ==(License left, License right)
        {
            return EqualityComparer<License>.Default.Equals(left, right);
        }

        public static bool operator !=(License left, License right)
        {
            return !(left == right);
        }
    }
}
