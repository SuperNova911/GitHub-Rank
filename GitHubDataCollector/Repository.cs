using System;
using System.Collections.Generic;
using System.Text;

namespace GitHubDataCollector
{
    public sealed class Repository
    {
        public long Id { get; private set; }
        public string NodeId { get; private set; }
        public long OwnerId { get; private set; }
        public string Name { get; private set; }
        public string Description { get; private set; }
        public string Language { get; private set; }
        public string LicenseKey { get; private set; }
        public int SubscribersCount { get; private set; }
        public int StargazersCount { get; private set; }
        public int ForksCount { get; private set; }
        public bool Fork { get; private set; }
        public bool Archived { get; private set; }
        public string HtmlUrl { get; private set; }
        public string ApiUrl { get; private set; }
        public DateTime CreatedAt { get; private set; }
        public DateTime UpdatedAt { get; private set; }
        public long ParentId { get; private set; }
        public long SourceId { get; private set; }
        public DateTime FetchedAt { get; private set; }
        public bool Valid { get; private set; }

        public Repository(Octokit.Repository repository, bool valid)
        {
            Id = repository.Id;
            NodeId = repository.NodeId;
            OwnerId = repository.Owner.Id;
            Name = repository.Name ?? throw new ArgumentNullException(nameof(repository.Name));
            Description = repository.Description ?? string.Empty;
            Language = repository.Language ?? string.Empty;
            LicenseKey = repository.License != null ? repository.License.Key : null;
            SubscribersCount = repository.SubscribersCount;
            StargazersCount = repository.StargazersCount;
            ForksCount = repository.ForksCount;
            Fork = repository.Fork;
            Archived = repository.Archived;
            HtmlUrl = repository.HtmlUrl ?? string.Empty;
            ApiUrl = repository.Url ?? string.Empty;
            CreatedAt = repository.CreatedAt.DateTime;
            UpdatedAt = repository.UpdatedAt.DateTime;
            ParentId = repository.Parent != null ? repository.Parent.Id : -1;
            SourceId = repository.Source != null ? repository.Source.Id : -1;
            FetchedAt = DateTime.Now;
            Valid = valid;
        }

        public Repository(long id, string nodeId, long ownerId, string name, string description, string language, string licenseKey, int subscribersCount, int stargazersCount, int forksCount, bool fork, bool archived, string htmlUrl, string apiUrl, DateTime createdAt, DateTime updatedAt, long parentId, long sourceId, DateTime fetchedAt, bool valid)
        {
            Id = id;
            NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
            OwnerId = ownerId;
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Description = description ?? throw new ArgumentNullException(nameof(description));
            Language = language ?? throw new ArgumentNullException(nameof(language));
            LicenseKey = licenseKey;
            SubscribersCount = subscribersCount;
            StargazersCount = stargazersCount;
            ForksCount = forksCount;
            Fork = fork;
            Archived = archived;
            HtmlUrl = htmlUrl ?? throw new ArgumentNullException(nameof(htmlUrl));
            ApiUrl = apiUrl ?? throw new ArgumentNullException(nameof(apiUrl));
            CreatedAt = createdAt;
            UpdatedAt = updatedAt;
            ParentId = parentId;
            SourceId = sourceId;
            FetchedAt = fetchedAt;
            Valid = valid;
        }
    }
}
