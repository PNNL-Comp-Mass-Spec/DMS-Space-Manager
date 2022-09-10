using System.Runtime.InteropServices;

namespace Space_Manager
{
    /// <summary>
    /// Class for kernel32 method signatures
    /// </summary>
    internal static class RemoteShareInfo
    {
        [DllImport("Kernel32", SetLastError = true, CharSet = CharSet.Auto)]
        [return: MarshalAs(UnmanagedType.Bool)]

        public static extern bool GetDiskFreeSpaceEx
        (
            string lpszPath, // Must name a remote share and must end with '\'
            ref long lpFreeBytesAvailable,
            ref long lpTotalNumberOfBytes,
            ref long lpTotalNumberOfFreeBytes
        );
    }
}
