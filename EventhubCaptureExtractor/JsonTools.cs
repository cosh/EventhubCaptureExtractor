/*
MIT License

Copyright (c) 2017 Henning Rauch

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

using Microsoft.Analytics.Types.Sql;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EventHubExtractor
{
    public static class JsonTools
    {
        private static readonly IEnumerable<KeyValuePair<string, string>> EMPTYRESULT = Enumerable.Empty<KeyValuePair<string, string>>();

        public static SqlMap<string, string> ExtractBody(String json)
        {

            JToken root = null;
            try
            {
                root = string.IsNullOrEmpty(json) ? new JObject() : JToken.Parse(json);
            }
            catch (Newtonsoft.Json.JsonReaderException e)
            {
                Console.WriteLine("Error while reading json string");
                return SqlMap.Create(EMPTYRESULT);
            }

            var o = root as JObject;

            IEnumerable<JToken> tokens;
            if (o != null)
            {
                tokens = o.PropertyValues();
            }
            else
            {
                tokens = root.Children();
            }

            return SqlMap.Create(TransformTokens(tokens));
        }

        private static IEnumerable<KeyValuePair<string, string>> TransformTokens(IEnumerable<JToken> tokens)
        {
            foreach (var token in tokens)
            {
                // Tuple(path, value)
                yield return new KeyValuePair<string, string>(token.Path, token.ToString());
            }
        }
    }
}