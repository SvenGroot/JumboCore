// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides information about the free disk space of a drive.
    /// </summary>
    /// <remarks>
    /// This class uses <see cref="DriveInfo"/> on .Net, and <strong>Mono.Unix.UnixDriveInfo</strong> on Mono.
    /// </remarks>
    public class DriveSpaceInfo
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DriveSpaceInfo"/> class.
        /// </summary>
        /// <param name="drivePath">The path of the drive for which to get the amount of space.</param>
        public DriveSpaceInfo(string drivePath)
        {
            if( drivePath == null )
                throw new ArgumentNullException(nameof(drivePath));

            if( RuntimeEnvironment.RuntimeType == RuntimeEnvironmentType.Mono )
                GetDriveSpaceMono(drivePath);
            else
                GetDriveSpace(drivePath);
        }

        /// <summary>
        /// Gets the amount of available free space.
        /// </summary>
        /// <value>
        /// The available free space, in bytes.
        /// </value>
        /// <remarks>
        /// This property may be different from <see cref="AvailableFreeSpace"/> because this property takes into account disk quotas.
        /// </remarks>
        public long AvailableFreeSpace { get; private set; }

        /// <summary>
        /// Gets the total amount of free space.
        /// </summary>
        /// <value>
        /// The total free space, in bytes.
        /// </value>
        /// <remarks>
        /// This property indicates the total amount of free space available on the drive, not just what is available to the current user.
        /// </remarks>
        public long TotalFreeSpace { get; private set; }

        /// <summary>
        /// Gets the total size of the drive.
        /// </summary>
        /// <value>
        /// The total size of the drive, in bytes.
        /// </value>
        public long TotalSize { get; private set; }

        private void GetDriveSpace(string drivePath)
        {
            DriveInfo drive = new DriveInfo(drivePath);
            AvailableFreeSpace = drive.AvailableFreeSpace;
            TotalFreeSpace = drive.TotalFreeSpace;
            TotalSize = drive.TotalSize;
        }

        private void GetDriveSpaceMono(string drivePath)
        {
            Type unixDriveInfo = Type.GetType("Mono.Unix.UnixDriveInfo, Mono.Posix, Version=2.0.0.0, Culture=neutral, PublicKeyToken=0738eb9f132ed756");
            if( unixDriveInfo != null )
            {
                object drive = Activator.CreateInstance(unixDriveInfo, drivePath);
                AvailableFreeSpace = (long)unixDriveInfo.GetProperty("AvailableFreeSpace").GetValue(drive, null);
                TotalFreeSpace = (long)unixDriveInfo.GetProperty("TotalFreeSpace").GetValue(drive, null);
                TotalSize = (long)unixDriveInfo.GetProperty("TotalSize").GetValue(drive, null);
            }
        }
    }
}
