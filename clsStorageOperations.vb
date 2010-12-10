Imports System.IO
Imports SpaceManagerNet.MgrSettings
Imports PRISM.Logging
Imports PRISM.Files
Imports System.Security.Cryptography
Imports DataFolderAccessVerifierDLL

Public Class clsStorageOperations

#Region "Enums"
	Public Enum ArchiveCompareResults
		Compare_Equal
		Compare_Not_Equal
		Compare_Error
	End Enum
#End Region

#Region "Module variables"
	' access to the logger
	Private m_logger As ILogger

	' access to mgr parameters
	Private m_mgrParams As IMgrParams

	' message for caller
	Private m_message As String

	' client perspective flag
	Private m_clientPerspective As Boolean = False

	' debug level
	Private m_DebugLevel As Integer = 0

	' archive data access verification
	Private WithEvents m_AccessVerifier As clsFolderAccessVerifier
#End Region

#Region "Properties"
	Public ReadOnly Property Message() As String
		Get
			Return m_message
		End Get
	End Property
#End Region

	Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger, ByVal DebugLevel As Integer)

		' constructor
		m_mgrParams = mgrParams
		m_logger = logger
		m_DebugLevel = DebugLevel

		m_clientPerspective = (m_mgrParams.GetParam("programcontrol", "perspective") = "client")
		m_AccessVerifier = New clsFolderAccessVerifier

		With m_AccessVerifier
			.FolderTimeoutSeconds = 15
			.FileTimeoutSeconds = 5
			.RecurseSubfolders = True
			.RecurseFoldersMaxLevels = 0
			.IgnoreTimeoutErrorsWhenRecursing = True
		End With

	End Sub

	Public Function CompareDatasetFolders(ByVal DsName As String, ByVal SvrDSNamePath As String, ByVal SambaDsNamePath As String) As ArchiveCompareResults

		'Compares contents of server and archive dataset folders

		Dim ServerFiles As System.Collections.ArrayList
		Dim ArchFileName As String
		Dim Msg As String

		'Verify server dataset folder exists
		If Not Directory.Exists(SvrDSNamePath) Then
			Msg = "clsUpdateOps.CompareDatasetFolders, folder " & SvrDSNamePath & " not found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return ArchiveCompareResults.Compare_Error
		End If

		'Verify Samba dataset folder exists
		If Not Directory.Exists(SambaDsNamePath) Then
			Msg = "clsUpdateOps.CompareDatasetFolders, folder " & SambaDsNamePath & " not found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return ArchiveCompareResults.Compare_Error
		End If

		'Get a list of all files in the results folder and subfolders
		Try
			Dim DirsToScan() As String = {SvrDSNamePath}
			Dim DirScanner As New DirectoryScanner(DirsToScan)
			DirScanner.PerformScan(ServerFiles, "*.*")
		Catch ex As Exception
			Msg = "clsUpdateOps.CompareJobFolders; exception getting file listing: " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return ArchiveCompareResults.Compare_Error
		End Try

		'Loop through results folder file list, checking for archive copies and comparing if archive copy present
		For Each SvrFileName As String In ServerFiles
			'Convert the file name on the storage server to its equivalent in the archive
			ArchFileName = ConvertServerPathToArchivePath(SvrDSNamePath, SambaDsNamePath, SvrFileName)
			If ArchFileName.Length = 0 Then
				Msg = "File name not returned when converting from server path to archive path for file" & SvrFileName
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return ArchiveCompareResults.Compare_Error
			ElseIf ArchFileName = "Error" Then
				'Error was logged by called function, so just return
				Return ArchiveCompareResults.Compare_Error
			End If

			'Determine if file exists in archive
			If File.Exists(ArchFileName) Then
				'File exists in archive, so compare the server and archive versions
				Dim CompRes As ArchiveCompareResults = CompareTwoFiles(SvrFileName, ArchFileName)
				If CompRes = ArchiveCompareResults.Compare_Equal Then
					'Do nothing
				ElseIf CompRes = ArchiveCompareResults.Compare_Not_Equal Then
					'An update is required
					Msg = "Update required. Server file " & SvrFileName & " doesn't match archive file " & ArchFileName
					m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, True)
					Return ArchiveCompareResults.Compare_Not_Equal
				Else
					'There was a problem with the file comparison. Error message has already been logged, so just exit
					Return ArchiveCompareResults.Compare_Error
				End If
			Else
				'File doesn't exist in archive, so update will be required
				Msg = "Update required. No copy of server file " & SvrFileName & " found in archive"
				m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, True)
				Return ArchiveCompareResults.Compare_Not_Equal
			End If
		Next

		'All finished, so return
		Return ArchiveCompareResults.Compare_Equal

	End Function

	Private Function CompareTwoFiles(ByVal InpFile1 As String, ByVal InpFile2 As String) As ArchiveCompareResults

		' Compares two files via SHA hash
		Dim File1Hash As String		 'String version of InpFile1 hash
		Dim File2Hash As String		 'String version of InpFile2 hash

		'Get hash's for both files
		File1Hash = GenerateHashFromFile(InpFile1)
		If File1Hash = "" Then
			'There was a problem. Description has already been logged
			Return ArchiveCompareResults.Compare_Error
		End If
		File2Hash = GenerateHashFromFile(InpFile2)
		If File2Hash = "" Then
			'There was a problem. Description has already been logged
			Return ArchiveCompareResults.Compare_Error
		End If

		If File1Hash = File2Hash Then
			Return ArchiveCompareResults.Compare_Equal
		Else
			Return ArchiveCompareResults.Compare_Not_Equal			'Files not equal
		End If

	End Function

	Private Function GenerateHashFromFile(ByVal InpFileNamePath As String) As String

		' Generates hash code for specified input file
		Dim ByteHash() As Byte		 'Holds hash value returned from hash generator
		Dim HashGen As New SHA1CryptoServiceProvider
		Dim Msg As String

		'Verify input file exists
		If Not File.Exists(InpFileNamePath) Then
			Msg = "clsUpdateOps.GenerateHashFromFile; File not found: " & InpFileNamePath
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return ""
		End If

		Dim Fi As New FileInfo(InpFileNamePath)
		Dim FStream As Stream

		Try
			'Open the file as a stream for input to the hash class
			FStream = Fi.OpenRead
			'Get the file's hash
			ByteHash = HashGen.ComputeHash(FStream)
			Return BitConverter.ToString(ByteHash)
		Catch ex As Exception
			Msg = "clsUpdateOps.GenerateHashFromFile; Exception generating hash for file " & InpFileNamePath & ": " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return ""
		Finally
			If Not FStream Is Nothing Then
				FStream.Close()
			End If
		End Try

	End Function

	Private Function ConvertServerPathToArchivePath( _
	  ByVal DSPathSvr As String, ByVal DSPathArch As String, ByVal InpfileName As String) As String

		Dim Msg As String

		'Convert by replacing storage server path with archive path
		Try
			Dim TmpPath As String = InpfileName.Replace(DSPathSvr, DSPathArch)
			Return TmpPath
		Catch ex As Exception
			Msg = "Exception converting server path to archive path for file " & DSPathSvr & ": " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return "Error"
		End Try

	End Function

	Public Function PurgeDataset(ByVal PurgeParams As PurgeTask.IPurgeTaskParams) As PurgeTask.IPurgeTaskParams.CloseOutType

		'Delete contents of dataset folder

		'		Dim Result As PurgeTask.IPurgeTaskParams.CloseOutType
		Dim RetVal As Boolean
		Dim DsPathSvr As String
		Dim DsPathSamba As String
		Dim Msg As String
		Dim DsName As String = PurgeParams.GetParam("dataset")

		'Get path to dataset folder on server
		If m_clientPerspective Then
			'Manager is running on a client
			DsPathSvr = PurgeParams.GetParam("StorageVolExternal")
		Else
			'Manager is running on storage server
			DsPathSvr = PurgeParams.GetParam("StorageVol")
		End If
		DsPathSvr &= (PurgeParams.GetParam("storagePath") & PurgeParams.GetParam("datasetfolder"))

		'Get path to dataset folder in archive
		DsPathSamba = PurgeParams.GetParam("SambaStoragePath") & PurgeParams.GetParam("datasetfolder")

		'Determine if dataset folder in archive has been sent to tape (causes timeout errors)
		Msg = "Verifying archived folder " & DsPathSamba & " is on disk"
		m_logger.PostEntry(Msg, ILogger.logMsgType.logNormal, True)
		If m_AccessVerifier.ProcessFolder(DsPathSamba) Then
			'Success - just make a log entry
			Msg = "Archive folder " & DsPathSamba & " successfully verified"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logNormal, True)
		Else
			Msg = m_AccessVerifier.GetResultsSummary()
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
		End If

		m_logger.PostEntry("Verifying archive integrity, dataset " & DsPathSvr, ILogger.logMsgType.logNormal, True)
		Dim CompRes As clsStorageOperations.ArchiveCompareResults = CompareDatasetFolders(DsName, DsPathSvr, DsPathSamba)
		Select Case CompRes
			Case ArchiveCompareResults.Compare_Equal
				'Nothing needs to be done; continue with function
			Case ArchiveCompareResults.Compare_Error
				'Unable to perform comparison operation; set purge task failed
				'	Error was logged during comparison
				Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
			Case ArchiveCompareResults.Compare_Not_Equal
				'Sever/Archive mismatch; an archive update is required before purging
				Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_UPDATE_REQUIRED
		End Select

		'Purge the dataset folder by deleting contents
		m_logger.PostEntry("Purging dataset " & DsPathSvr, ILogger.logMsgType.logNormal, True)

		'Get a file listing for the dataset folder on the server
		Dim DsFiles() As String = Directory.GetFiles(DsPathSvr)
		If m_DebugLevel > 3 Then
			Msg = "Dataset " & DsName & ": " & DsFiles.GetLength(0).ToString & " files found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
		End If

		'Get a folder listing for the dataset folder on the server
		Dim DsFolders() As String = Directory.GetDirectories(DsPathSvr)
		If m_DebugLevel > 3 Then
			Msg = "Dataset " & DsName & ": " & DsFolders.GetLength(0).ToString & " folders found"
			m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
		End If

		'Verify at least 1 file or folder was found to purge
		If (DsFiles.GetLength(0) = 0) And (DsFolders.GetLength(0) = 0) Then
			'Nothing was found to purge. Something's rotten in DMS
			Msg = "No purgeable data found for datset " & DsName
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Delete the files in dataset folder
		For Each FileToDelete As String In DsFiles
			Try
#If DoDelete Then
				SetAttr(FileToDelete, FileAttribute.Normal)
				File.Delete(FileToDelete)
#End If
			Catch ex As Exception
				Msg = "Exception deleting file " & FileToDelete & "; " & ex.Message
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		Next
		If m_DebugLevel > 3 Then
			Msg = "Deleted files in dataset folder " & DsPathSvr
			m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
		End If

		'Delete the folders in the dataset folder, leaving the dataset folder itself intact
		For Each FolderToDelete As String In DsFolders
			Try
#If DoDelete Then
				Directory.Delete(FolderToDelete, True)
#End If
			Catch ex As Exception
				Msg = "Exception deleting folder " & FolderToDelete & "; " & ex.Message
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		Next
		If m_DebugLevel > 3 Then
			Msg = "Deleted folders in dataset folder " & DsPathSvr
			m_logger.PostEntry(Msg, ILogger.logMsgType.logDebug, True)
		End If

		'We got here, so log success and exit
		Msg = "Purged dataset " & DsName
		m_logger.PostEntry(Msg, ILogger.logMsgType.logNormal, False)
		Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Sub m_AccessVerifier_ErrorEvent(ByVal strMessage As String) Handles m_AccessVerifier.ErrorEvent
		m_logger.PostEntry(strMessage, ILogger.logMsgType.logError, True)
	End Sub

	Private Sub m_AccessVerifier_MessageEvent(ByVal strMessage As String) Handles m_AccessVerifier.MessageEvent
		m_logger.PostEntry(strMessage, ILogger.logMsgType.logNormal, True)
	End Sub

	Private Sub m_AccessVerifier_WarningEvent(ByVal strMessage As String) Handles m_AccessVerifier.WarningEvent
		m_logger.PostEntry(strMessage, ILogger.logMsgType.logWarning, True)
	End Sub
End Class
