using System;
using System.Collections.Generic;
using System.Text;

namespace GitHub_Data_Collector
{
    public sealed class User : Account
    {
        public bool SiteAdmin { get; private set; }
        public bool Suspended { get; private set; }
        public DateTime SuspendedAt { get; private set; }
        public DateTime UpdatedAt { get; private set; }

        public User(Octokit.User user, bool valid) : base(user, valid)
        {
            SiteAdmin = user.SiteAdmin;
            Suspended = user.Suspended;
            SuspendedAt = user.SuspendedAt.HasValue ? user.SuspendedAt.Value.DateTime : DateTime.MinValue;
            UpdatedAt = user.UpdatedAt.DateTime;
        }

        public User(long id, string nodeId, AccountType typeId, string login, int following, int followers, int publicRepos, string avatarUrl, string htmlUrl, string apiUrl, string blogUrl, string email, string bio, string company, string location, DateTime createdAt, DateTime fetchedAt, bool valid, bool siteAdmin, bool suspended, DateTime suspendedAt, DateTime updatedAt) : base(id, nodeId, typeId, login, following, followers, publicRepos, avatarUrl, htmlUrl, apiUrl, blogUrl, email, bio, company, location, createdAt, fetchedAt, valid)
        {
            SiteAdmin = siteAdmin;
            Suspended = suspended;
            SuspendedAt = suspendedAt;
            UpdatedAt = updatedAt;
        }
    }
}
