// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("General", "RCS1118:Mark local variable as const.", Justification = "Valid variable when compiling in debug mode", Scope = "member", Target = "~M:Space_Manager.MainProgram.ProcessDrive(System.Int32,Space_Manager.DriveData)~Space_Manager.MainProgram.DriveOpStatus")]
[assembly: SuppressMessage("General", "RCS1118:Mark local variable as const.", Justification = "Valid variable when compiling in debug mode", Scope = "member", Target = "~M:Space_Manager.StorageOperations.MarkPurgedJobs(System.Collections.Generic.IReadOnlyCollection{System.Int32})")]
[assembly: SuppressMessage("General", "RCS1118:Mark local variable as const.", Justification = "Valid variable when compiling in debug mode", Scope = "member", Target = "~M:Space_Manager.StorageOperations.PurgeDataset(Space_Manager.ITaskParams)~Space_Manager.EnumCloseOutType")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:Space_Manager.StorageOperations.CompareFileUsingSamba(System.String,System.String,Space_Manager.StorageOperations.udtDatasetInfoType,System.IO.FileSystemInfo)~Space_Manager.StorageOperations.ArchiveCompareResults")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.DbTask")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.DriveData")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.HashInfo")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.LoggerBase")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.MainProgram")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.MessageHandler")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.PurgeableFileSearcher")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.SpaceMgrTask")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.StatusData")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.StatusFile")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.StorageOperations")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.StorageOperations.udtDatasetInfoType")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:Space_Manager.UtilityMethods")]
