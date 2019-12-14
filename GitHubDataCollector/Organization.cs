using System;
using System.Collections.Generic;
using System.Text;

namespace GitHubDataCollector
{
    public sealed class Organization : Account
    {
        public Organization(Octokit.Account account, bool valid) : base(account, valid)
        {
        }

        public Organization(long id, string nodeId, AccountType typeId, string login, int following, int followers, int publicRepos, string avatarUrl, string htmlUrl, string apiUrl, string blogUrl, string email, string bio, string company, string location, DateTime createdAt, DateTime fetchedAt, bool valid) : base(id, nodeId, typeId, login, following, followers, publicRepos, avatarUrl, htmlUrl, apiUrl, blogUrl, email, bio, company, location, createdAt, fetchedAt, valid)
        {
        }
    }
}
