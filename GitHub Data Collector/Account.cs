using System;
using System.Collections.Generic;
using System.Text;

namespace GitHub_Data_Collector
{
    public abstract class Account
    {
        public long Id { get; private set; }
        public string NodeId { get; private set; }
        public AccountType TypeId { get; private set; }
        public string Login { get; private set; }
        public int Following { get; private set; }
        public int Followers { get; private set; }
        public int PublicRepos { get; private set; }
        public string AvatarUrl { get; private set; }
        public string HtmlUrl { get; private set; }
        public string ApiUrl { get; private set; }
        public string BlogUrl { get; private set; }
        public string Email { get; private set; }
        public string Bio { get; private set; }
        public string Company { get; private set; }
        public string Location { get; private set; }
        public DateTime CreatedAt { get; private set; }
        public DateTime FetchedAt { get; private set; }
        public bool Valid { get; private set; }

        public enum AccountType
        {
            User = 0, Organization = 1, Bot = 2
        }

        protected Account(Octokit.Account account, bool valid)
        {
            Id = account.Id;
            NodeId = account.NodeId;
            TypeId = account.Type.HasValue ? (AccountType)account.Type.Value : AccountType.User;
            Login = account.Login ?? throw new ArgumentNullException(nameof(account.Login));
            Following = account.Following;
            Followers = account.Followers;
            PublicRepos = account.PublicRepos;
            AvatarUrl = account.AvatarUrl ?? string.Empty;
            HtmlUrl = account.HtmlUrl ?? string.Empty;
            ApiUrl = account.Url ?? string.Empty;
            BlogUrl = account.Blog ?? string.Empty;
            Email = account.Email ?? string.Empty;
            Bio = account.Bio ?? string.Empty;
            Company = account.Company ?? string.Empty;
            Location = account.Location ?? string.Empty;
            CreatedAt = account.CreatedAt.DateTime;
            FetchedAt = DateTime.Now;
            Valid = valid;
        }

        protected Account(long id, string nodeId, AccountType typeId, string login, int following, int followers, int publicRepos, string avatarUrl, string htmlUrl, string apiUrl, string blogUrl, string email, string bio, string company, string location, DateTime createdAt, DateTime fetchedAt, bool valid)
        {
            Id = id;
            NodeId = nodeId ?? throw new ArgumentNullException(nameof(nodeId));
            TypeId = typeId;
            Login = login ?? throw new ArgumentNullException(nameof(login));
            Following = following;
            Followers = followers;
            PublicRepos = publicRepos;
            AvatarUrl = avatarUrl ?? throw new ArgumentNullException(nameof(avatarUrl));
            HtmlUrl = htmlUrl ?? throw new ArgumentNullException(nameof(htmlUrl));
            ApiUrl = apiUrl ?? throw new ArgumentNullException(nameof(apiUrl));
            BlogUrl = blogUrl ?? throw new ArgumentNullException(nameof(blogUrl));
            Email = email ?? throw new ArgumentNullException(nameof(email));
            Bio = bio ?? throw new ArgumentNullException(nameof(bio));
            Company = company ?? throw new ArgumentNullException(nameof(company));
            Location = location ?? throw new ArgumentNullException(nameof(location));
            CreatedAt = createdAt;
            FetchedAt = fetchedAt;
            Valid = valid;
        }

        public static Account ParseAccount(Octokit.User user, bool valid)
        {
            if (user.Type.HasValue)
            {
                switch (user.Type.Value)
                {
                    case Octokit.AccountType.User:
                        return new User(user, valid);
                    case Octokit.AccountType.Organization:
                        return new Organization(user, valid);

                    case Octokit.AccountType.Bot:
                    default:
                        Console.WriteLine($"관리되지 않는 {nameof(Octokit.AccountType)}입니다, {user.Type.Value}");
                        return new User(user, valid);
                }
            }
            else
            {
                Console.WriteLine($"{nameof(Octokit.AccountType)}이 null입니다, {nameof(user.Login)}: {user.Login}");
                return new User(user, valid);
            }
        }
    }
}
