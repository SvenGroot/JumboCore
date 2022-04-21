using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.IO
{
    /// <summary>
    /// Provides methods for saving and loading task inputs.
    /// </summary>
    public static class TaskInputUtility
    {
        /// <summary>
        /// Writes the task inputs.
        /// </summary>
        /// <param name="fileSystem">The file system to write the inputs to.</param>
        /// <param name="path">The path of the directory to write the output to.</param>
        /// <param name="stageId">The ID of the stage that these inputs belong to.</param>
        /// <param name="inputs">The inputs to write.</param>
        public static void WriteTaskInputs(FileSystemClient fileSystem, string path, string stageId, IEnumerable<ITaskInput> inputs)
        {
            if (fileSystem == null)
                throw new ArgumentNullException(nameof(fileSystem));
            if (inputs == null)
                throw new ArgumentNullException(nameof(inputs));
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            if (stageId == null)
                throw new ArgumentNullException(nameof(stageId));

            string splitsFile = GetSplitsFileName(fileSystem, path, stageId);
            string splitsIndexFile = GetSplitsIndexFileName(splitsFile);
            string locationsFile = GetLocationsFileName(fileSystem, path, stageId);

            using (BinaryWriter writer = new BinaryWriter(fileSystem.CreateFile(splitsFile)))
            using (BinaryWriter indexWriter = new BinaryWriter(fileSystem.CreateFile(splitsIndexFile)))
            using (BinaryWriter locationsWriter = new BinaryWriter(fileSystem.CreateFile(locationsFile)))
            {
                bool first = true;
                foreach (ITaskInput input in inputs)
                {
                    if (first)
                    {
                        writer.Write(input.GetType().AssemblyQualifiedName);
                    }
                    indexWriter.Write(writer.BaseStream.Position);
                    input.Write(writer);
                    writer.Flush();

                    if (input.Locations == null)
                        WritableUtility.Write7BitEncodedInt32(locationsWriter, 0);
                    else
                    {
                        WritableUtility.Write7BitEncodedInt32(locationsWriter, input.Locations.Count);
                        foreach (string location in input.Locations)
                        {
                            locationsWriter.Write(location);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Reads the task input locations.
        /// </summary>
        /// <param name="fileSystem">The file system to read the locations from.</param>
        /// <param name="path">The path of the directory containing the locations file.</param>
        /// <param name="stageId">The ID of the stage whose task input locations to read.</param>
        /// <returns>A list of input locations for each task.</returns>
        public static IList<string[]> ReadTaskInputLocations(FileSystemClient fileSystem, string path, string stageId)
        {
            if (fileSystem == null)
                throw new ArgumentNullException(nameof(fileSystem));
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            if (stageId == null)
                throw new ArgumentNullException(nameof(stageId));

            string locationsFile = GetLocationsFileName(fileSystem, path, stageId);

            List<string[]> result = new List<string[]>();
            using (BinaryReader reader = new BinaryReader(fileSystem.OpenFile(locationsFile)))
            {
                while (reader.BaseStream.Position < reader.BaseStream.Length)
                {
                    int locationsCount = WritableUtility.Read7BitEncodedInt32(reader);
                    string[] locations = new string[locationsCount];
                    for (int x = 0; x < locationsCount; ++x)
                        locations[x] = reader.ReadString();
                    result.Add(locations);
                }
            }

            return result;
        }

        /// <summary>
        /// Reads the task input for the specified split.
        /// </summary>
        /// <param name="fileSystem">The file system containing the splits file.</param>
        /// <param name="path">The path of the directory containing the splits file.</param>
        /// <param name="stageId">The ID of the stage whose input to read.</param>
        /// <param name="splitIndex">The index of the split.</param>
        /// <returns>The task input.</returns>
        public static ITaskInput ReadTaskInput(FileSystemClient fileSystem, string path, string stageId, int splitIndex)
        {
            if (fileSystem == null)
                throw new ArgumentNullException(nameof(fileSystem));
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            if (stageId == null)
                throw new ArgumentNullException(nameof(stageId));
            if (splitIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(splitIndex));

            string splitsFile = GetSplitsFileName(fileSystem, path, stageId);
            string splitsIndexFile = GetSplitsIndexFileName(splitsFile);

            using (BinaryReader reader = new BinaryReader(fileSystem.OpenFile(splitsFile)))
            using (BinaryReader indexReader = new BinaryReader(fileSystem.OpenFile(splitsIndexFile)))
            {
                string typeName = reader.ReadString();
                indexReader.BaseStream.Position = splitIndex * sizeof(long);
                long offset = indexReader.ReadInt64();
                reader.BaseStream.Position = offset;
                ITaskInput result = (ITaskInput)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(Type.GetType(typeName, true));
                result.Read(reader);

                return result;
            }
        }

        private static string GetLocationsFileName(FileSystemClient fileSystem, string path, string stageId)
        {
            return fileSystem.Path.Combine(path, stageId + "_splitlocations");
        }

        private static string GetSplitsIndexFileName(string splitsFile)
        {
            return splitsFile + ".index";
        }

        private static string GetSplitsFileName(FileSystemClient fileSystem, string path, string stageId)
        {
            return fileSystem.Path.Combine(path, stageId + "_splits");
        }
    }
}
