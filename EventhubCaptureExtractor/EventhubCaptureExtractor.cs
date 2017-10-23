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

using System;
using System.Collections.Generic;
using Microsoft.Analytics.Interfaces;
using System.IO;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.Hadoop.Avro;

namespace EventHubExtractor
{
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class EventhubCaptureExtractor : IExtractor
    {
        private const string OFFSET = "Offset";
        private const string SystemProperties = "SystemProperties";
        private const string TIME = "EnqueuedTimeUtc";
        private const string PROPERTIES = "Properties";
        private const string SEQUENCENUMBER = "SequenceNumber";
        private const string BODY = "Body";

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            //Reading and deserializing data.
            //Creating a memory stream buffer.
            using (var buffer = new MemoryStream())
            {
                input.BaseStream.CopyTo(buffer);
                buffer.Position = 0;

                buffer.Seek(0, SeekOrigin.Begin);

                using (var reader = AvroContainer.CreateGenericReader(buffer))
                {
                    using (var streamReader = new SequentialReader<object>(reader))
                    {
                        var avrorecords = streamReader.Objects.GetEnumerator();

                        try
                        {
                            avrorecords.MoveNext();
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            Console.WriteLine("Empty file");
                            yield break;
                        }

                        do
                        {
                            var aEventData = avrorecords.Current;
                            yield return TransformEvent((AvroRecord)aEventData, output).AsReadOnly();
                        } while (avrorecords.MoveNext());

                    }
                }
            }
        }

        private IUpdatableRow TransformEvent(AvroRecord aEventData, IUpdatableRow output)
        {
            var body = aEventData[BODY];
            output.Set(BODY, body);
            return output;
        }
    }
}
