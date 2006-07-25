Imports System.IO
Imports System.Text.RegularExpressions
Imports SpaceManagerNet.MgrSettings
Imports PRISM.Logging
Imports Dart.PowerTCP.SecureFtp

Public Class clsStorageOperations

	' constructor
	Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger)
		m_mgrParams = mgrParams
		m_logger = logger

		m_clientPerspective = (m_mgrParams.GetParam("programcontrol", "perspective") = "client")
	End Sub

	' access to the logger
	Private m_logger As ILogger

	' access to mgr parameters
	Private m_mgrParams As IMgrParams

	' message for caller to get
	Private m_message As String

	' client perspective flag
	Private m_clientPerspective As Boolean = False

	'Security settings
	Private m_UseTLS As Boolean = False
	Private m_ServerPort As Integer = 21

	Public ReadOnly Property Message() As String
		Get
			Return m_message
		End Get
	End Property

	' delete raw spectra files from dataset folder
	Public Function PurgeDataset(ByVal pp As PurgeTask.IPurgeTaskParams) As PurgeTask.IPurgeTaskParams.CloseOutType

		Dim result As PurgeTask.IPurgeTaskParams.CloseOutType
		Dim RetVal As Boolean

		' get full path to dataset folder
		'
		Dim DSPath As String
		If m_clientPerspective Then
			DSPath &= pp.GetParam("StorageVolExternal")
		Else
			DSPath &= pp.GetParam("StorageVol")
		End If

		DSPath &= pp.GetParam("storagePath")
		DSPath &= pp.GetParam("Folder")

		m_logger.PostEntry("Purging dataset " & DSPath, ILogger.logMsgType.logNormal, True)

		Select Case pp.GetParam("RawDataType").ToLower
			Case "zipped_s_folders"
				RetVal = DeleteSFolders(DSPath)
				Return HandlePurgeResults(RetVal, DSPath)
			Case "dot_raw_files"
				RetVal = DeleteRawFile(DSPath)
				Return HandlePurgeResults(RetVal, DSPath)
			Case Else
				m_message = "Dataset " & DSPath & ", Invalid data type: " & pp.GetParam("RawDataType")
				m_logger.PostEntry(m_message, ILogger.logMsgType.logError, False)
				Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
		End Select

	End Function

	' copy raw spectra files from archive to dataset folder
	Public Function UnpurgeDataset(ByVal up As UnpurgeTask.IUnpurgeTaskParams) As UnpurgeTask.IUnpurgeTaskParams.CloseOutType
		Dim result As UnpurgeTask.IUnpurgeTaskParams.CloseOutType
		Dim mypath As String
		Dim Succeeded As Boolean = True

		Try
			' get full path to dataset folder
			'
			If m_clientPerspective Then
				mypath &= up.GetParam("StorageVolExternal")
			Else
				mypath &= up.GetParam("StorageVol")
			End If

			mypath &= up.GetParam("storagePath")
			mypath &= up.GetParam("Folder")


			m_logger.PostEntry("Unpurging dataset " & mypath, ILogger.logMsgType.logNormal, True)

			Dim archivePath As String = up.GetParam("ArchivePath") & "/" & up.GetParam("Folder")

			'Unpurge the dataset
			DoFTP(mypath, archivePath, up.GetParam("RawDataType"))

			If m_message <> "" Then
				'Something went wrong
				Succeeded = False
			End If
		Catch e As Exception
			m_message = e.Message
			Succeeded = False
		End Try

		If Succeeded Then
			m_logger.PostEntry("Unpurged dataset " & mypath, ILogger.logMsgType.logNormal, False)
			result = UnpurgeTask.IUnpurgeTaskParams.CloseOutType.CLOSEOUT_SUCCESS
		Else
			m_logger.PostEntry("Error unpurging dataset " & mypath & _
			 ": " & m_message, ILogger.logMsgType.logError, False)
			result = UnpurgeTask.IUnpurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return result

	End Function

	Private Sub DoFTP(ByVal storagePath As String, ByVal archivePath As String, ByVal PurgeFileType As String)

		Dim fileSpec As String
		Dim s() As String = Split(archivePath, ";")
		Dim server As String = s(0)
		Dim sourcePath As String = ""
		Dim TempPassword As String

		'Set the filespec
		Select Case PurgeFileType.ToLower
			Case "zipped_s_folders"
				fileSpec = "S*.zip"
			Case "dot_raw_files"
				fileSpec = "*.raw"
			Case Else
				'Should never get here
				m_message = "Invalid file spec: " & PurgeFileType
				Exit Sub
		End Select

		m_UseTLS = CBool(m_mgrParams.GetParam("archive", "usetls"))
		m_ServerPort = CInt(m_mgrParams.GetParam("archive", "serverport"))
		If s.Length > 1 Then
			sourcePath = s(1)
		End If

		Dim FTP1 As New Dart.PowerTCP.SecureFtp.Ftp
		FTP1.Username = m_mgrParams.GetParam("archive", "username")
		TempPassword = m_mgrParams.GetParam("archive", "password")
		FTP1.Password = DecodePassword(TempPassword)
		FTP1.Server = server
		FTP1.FileType = FileType.Image		' binary mode
		FTP1.DoEvents = False
		FTP1.Passive = True		' use "PASV" on connection instead of "PORT"
		FTP1.Restart = False		' file has to come in one transfer, no partials with retrys
		If m_UseTLS Then
			FTP1.Security = Security.ExplicitControlOnly
			FTP1.ServerPort = m_ServerPort
			FTP1.UseAuthentication = False
		Else
			FTP1.Security = Security.None
		End If

		'The DART FTP control doesn't actually open the connection until the first command is sent,
		'	so send a NOP and verify communication is OK
		Try
			If m_UseTLS Then
				'These two commands turn off the data channel encryption after logon for secure connections
				FTP1.Invoke(FtpCommand.Null, "PBSZ 0")
				FTP1.Invoke(FtpCommand.Null, "PROT C")
			End If
			FTP1.Invoke(FtpCommand.NoOp)
		Catch ex As Exception
			m_message = "Error opening FTP connection, " & ex.Message
			'Verify the error string doesn't contain a clear-text version of the password
			m_message = StripPwd(m_message)
			FTP1.Dispose()
			Exit Sub
		End Try

		'Retrieve the dataset files
		Try
			Dim Results() As FtpFile
			Results = FTP1.Get(sourcePath, fileSpec, storagePath, False)
			ReportFTPResults(Results)
			m_message = ""
		Catch ex As Exception
			m_message = "ERROR: " + Replace(ex.Message, vbCrLf, " ")
			m_message = StripPwd(m_message)
			FTP1.Dispose()
		End Try

	End Sub

	Private Sub ReportFTPResults(ByVal Results() As FtpFile)
		Dim Aborted As Boolean = False
		If Results.GetLength(0) = 0 Then
			m_logger.PostEntry("No matching files found.", ILogger.logMsgType.logNormal, True)
		Else
			' Check each result to see if there were any errors
			Dim f As FtpFile
			Dim goodcnt As Long
			goodcnt = Results.GetLength(0)
			For Each f In Results
				' If the exception property is true, display the error message
				If Not f.Exception Is Nothing Then
					m_logger.PostEntry("File error (" + f.RemoteFileName + "): " + Replace(f.Exception.Message, vbCrLf, " "), ILogger.logMsgType.logNormal, True)
					goodcnt -= 1
				ElseIf f.Count = -1 Then
					' If Count is -1, it means that AbortTransfer was called
					' before this file started transferring
					Aborted = True
					goodcnt -= 1
				ElseIf f.Position <> f.Length Then
					' If Position is not equal to length it means that 
					' AbortTransfer was called while this file was transferring
					m_logger.PostEntry("Transfer was aborted while retrieving " + f.LocalFileName, ILogger.logMsgType.logNormal, True)
					Aborted = True
					goodcnt -= 1
				End If
			Next
			If Aborted Then
				m_logger.PostEntry("Transfer aborted. " + CStr(Results.GetLength(0) - goodcnt) + " were skipped.", ILogger.logMsgType.logNormal, True)
			End If
			m_logger.PostEntry("Succesfully retrieved " + goodcnt.ToString + " files.", ILogger.logMsgType.logNormal, True)
		End If
	End Sub

	Private Function DecodePassword(ByVal EnPwd As String) As String

		'Decrypts password received from ini file
		' Password was created by alternately subtracting or adding 1 to the ASCII value of each character

		Dim CharCode As Byte
		Dim TempStr As String
		Dim Indx As Integer

		TempStr = ""

		Indx = 1
		Do While Indx <= Len(EnPwd)
			CharCode = CByte(Asc(Mid(EnPwd, Indx, 1)))
			If Indx Mod 2 = 0 Then
				CharCode = CharCode - CByte(1)
			Else
				CharCode = CharCode + CByte(1)
			End If
			TempStr = TempStr & Chr(CharCode)
			Indx = Indx + 1
			Application.DoEvents()
		Loop

		Return TempStr

	End Function

	Private Function StripPwd(ByVal InpStr As String) As String

		'Replaces the password returned by the Dart FTP control error message with "xxxxx"

		Return Regex.Replace(InpStr, "PASS \w*", "PASS xxxxxxx")

	End Function

	Private Function DeleteSFolders(ByVal InpPath As String) As Boolean


		'Deletes zipped s-folders from FTICR dataset folders
		Dim deletedSomething As Boolean = False
		Try
			' get list of files in dataset folder
			'
			Dim di As New DirectoryInfo(InpPath)
			Dim fil As FileInfo() = di.GetFiles()
			Dim fi As FileInfo

			' traverse the list and delete files that match the pattern
			'
			For Each fi In fil
				' look for zipped 's' folders and delete the ones found
				'
				If Regex.Match(fi.Name, "s\d*\.zip", RegexOptions.IgnoreCase).Success Then
					Dim msg As String = "Purged file:" & fi.Name & " (" & fi.Length.ToString & ")"
					m_logger.PostEntry(msg, ILogger.logMsgType.logNormal, True)
					deletedSomething = True
					fi.Delete()					'DANGER! DANGER! DANGER!
				End If
			Next
		Catch e As Exception
			m_message = e.Message
			Return False
		End Try

		If deletedSomething Then
			Return True
		Else
			m_message = "No files"
			Return False
		End If

	End Function

	Private Function DeleteRawFile(ByVal InpPath As String) As Boolean

		'Deletes all .raw files from dataset folder
		Dim RawFile As String

		' get list of *.raw files in dataset folder
		Dim RawFiles() As String = Directory.GetFiles(InpPath, "*.raw")

		If RawFiles.Length = 0 Then
			'No files found
			m_message = "No files"
			Return False
		End If

		'Delete all *.raw files found
		'
		For Each RawFile In RawFiles
			Try
				File.Delete(RawFile)				 'DANGER! DANGER! DANGER!
				Dim msg As String = "Purged file:" & RawFile
				m_logger.PostEntry(msg, ILogger.logMsgType.logNormal, True)
			Catch e As Exception
				m_message = e.Message
				Return False
			End Try
		Next

		Return True

	End Function

	Private Function HandlePurgeResults(ByVal RetVal As Boolean, ByVal DSPath As String) As PurgeTask.IPurgeTaskParams.CloseOutType

		'Common function to handle the results of a purge operation, regardless of data type
		If RetVal Then
			'Successful purge
			m_logger.PostEntry("Purged dataset " & [DSPath], ILogger.logMsgType.logNormal, False)
			Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_SUCCESS
		ElseIf m_message.IndexOf("No files") >= 0 Then
			'No files found
			m_message = "Dataset " & [DSPath] & ", No purgable files were found in dataset folder"
			m_logger.PostEntry(m_message, ILogger.logMsgType.logWarning, False)
			Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
		Else
			'Error occurred
			'TODO: Do we need log entry here?
			Return PurgeTask.IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
		End If

	End Function

End Class
