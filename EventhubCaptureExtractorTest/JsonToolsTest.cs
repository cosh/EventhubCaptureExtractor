﻿/*
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

using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.UnitTest;
using System.Collections.Generic;
using EventHubExtractor;

namespace JsonToolsTest
{
    [TestClass]
    public class JsonToolsTest
    {

        [TestMethod]
        public void ValidJsonFlat()
        {
            var result = JsonTools.ExtractBody("{\"id\":\"file\",\"value\":\"File\"}");

            Assert.AreEqual(2, result.Count);
        }
        
        [TestMethod]
        public void ValidJsonDeep()
        {
            var result = JsonTools.ExtractBody("{\"firstName\":\"John\",\"address\":{\"streetAddress\":\"21 2nd Street\"}}");
            Assert.AreEqual(2, result.Count);
        }

        [TestMethod]
        public void InValidJson()
        {
            var result = JsonTools.ExtractBody("{\"id\":\"file\" \"value\":\"File\"");
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public void EmptyString()
        {
            var result = JsonTools.ExtractBody("");
            Assert.AreEqual(0, result.Count);

            result = JsonTools.ExtractBody(null);
            Assert.AreEqual(0, result.Count);
        }
    }
}
