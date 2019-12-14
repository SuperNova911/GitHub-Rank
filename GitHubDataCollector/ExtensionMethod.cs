using System;
using System.Collections.Generic;
using System.Text;

namespace GitHubDataCollector
{
    public static class ExtensionMethod
    {
        public static IEnumerable<List<T>> Split<T>(this List<T> list, int splitSize)
        {
            if (list.Count == 0)
            {
                yield return list;
            }

            splitSize = Math.Clamp(splitSize, 1, list.Count);

            for (int i = 0; i < list.Count; i += splitSize)
            {
                yield return list.GetRange(i, Math.Min(splitSize, list.Count - i));
            }
        }
    }
}
