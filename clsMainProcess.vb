Imports System.IO
Imports System.Management
Imports SpaceManagerNet.MgrSettings
Imports PRISM.Logging
Imports SpaceManagerNet.PurgeTask


Public Class clsMainProcess

#Region "Constants"
	Const SPACE_CHECK_ERROR As Integer = -1
	Const SPACE_CHECK_ABOVE_THRESHOLD As Integer = 1
	Const SPACE_CHECK_BELOW_THRESHOLD As Integer = 0
#End Region

#Region "Member Variables"
	Private myMgrSettings As clsSpaceMgrSettings
	Private myLogger As ILogger
	Private myPurgeTask As clsPurgeTask
	Private myStorageOperations As clsStorageOperations
	Private m_clientPerspective As Boolean = False	 'TRUE = client; FALSE = server
	Private m_maxRepetitions As Integer = 1
	Private Shared m_StartupClass As clsMainProcess
	Private m_IniFileChanged As Boolean = False
	Private WithEvents m_FileWatcher As New FileSystemWatcher
	Private m_IniFileName As String = "SpaceManager.xml"
	Private m_MgrActive As Boolean = True
	Private m_DebugLevel As Integer = 0
#End Region

	Private Function GetIniFilePath(ByVal IniFileName As String) As String

		' create full path to ini file
		Dim fi As New FileInfo(Application.ExecutablePath)
		Return Path.Combine(fi.DirectoryName, IniFileName)

	End Function

	Private Sub DoSetup()
		' create and initialize the objects needed to do space management

		' create the object that will manage the space manager parameters
		myMgrSettings = New clsSpaceMgrSettings(GetIniFilePath(m_IniFileName))

		' client or server perspective?
		m_clientPerspective = (myMgrSettings.GetParam("programcontrol", "perspective") = "client")

		' maximum number of passes through the main purge loop
		m_maxRepetitions = Integer.Parse(myMgrSettings.GetParam("programcontrol", "maxrepetitions"))

		' Manager active?
		m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))

		' Debug level
		m_DebugLevel = CInt(myMgrSettings.GetParam("programcontrol", "debuglevel"))

		' create the object that will manage the logging
		Dim dbl As New clsDBLogger
		dbl.LogFilePath = myMgrSettings.GetParam("logging", "logfilename")
		dbl.ConnectionString = myMgrSettings.GetParam("DatabaseSettings", "ConnectionString")
		dbl.ModuleName = myMgrSettings.GetParam("programcontrol", "modulename")
		dbl.LogExecutableName = False
		dbl.LogExecutableVersion = False
		myLogger = New clsQueLogger(dbl)
		dbl = Nothing

		' create the object that will manager storage space operations
		myStorageOperations = New clsStorageOperations(myMgrSettings, myLogger, m_DebugLevel)

		' create the object that will manage the purge tasks
		myPurgeTask = New clsPurgeTask(myMgrSettings, myLogger, m_DebugLevel)

	End Sub

	Private Function SpaceAboveThreshold(ByVal machine As String, ByVal DriveData() As String) As Integer

		'Determine if free space on disk is above acceptable threshold. Returns +1 if space above threshold,
		'	0 if space below threshold, and -1 for error	

		Dim AvailableSpace As Double
		Dim request As String
		Dim Threshold As Long = Long.Parse(DriveData(1))
		Dim RetCode As Integer

		' get WMI object representing drive and populate it
		' and use it to get free space and convert result to gigabytes
		If m_clientPerspective Then
			request = "\\" & machine & "\root\cimv2:win32_logicaldisk.deviceid=""" & DriveData(0) & """"
		Else
			request = "win32_logicaldisk.deviceid=""" & DriveData(0) & """"
		End If

		Try
			Dim disk As New ManagementObject(request)
			disk.Get()
			AvailableSpace = System.Convert.ToDouble(disk("FreeSpace"))
			AvailableSpace /= 2 ^ 30			 'convert raw byte count to gigabytes
			If AvailableSpace > Threshold Then
				RetCode = SPACE_CHECK_ABOVE_THRESHOLD
			Else
				RetCode = SPACE_CHECK_BELOW_THRESHOLD
			End If
		Catch ex As Exception
			Dim Msg As String = "Exception getting free disk space, drive " & DriveData(0) & "; " & ex.Message
			myLogger.PostEntry(Msg, ILogger.logMsgType.logError, True)
			RetCode = SPACE_CHECK_ERROR
		End Try

		If m_DebugLevel > 3 Then
			myLogger.PostEntry("Space Threshold: " & Threshold.ToString & ", Avail space: " & AvailableSpace.ToString("####0.0"), _
			 ILogger.logMsgType.logDebug, True)
		End If

		Return RetCode

	End Function

	Public Sub DoSpaceManagementForOneDrive(ByVal Machine As String, ByVal Drive As String)

		' Manage the storage space for the given drive on the storage server
		Dim rp As PurgeTask.IPurgeTaskParams.CloseOutType
		Dim DriveData() As String		 'This will be a 2-element array, containing drive letter and threshold

		Dim LoopCounter As Integer = 0
		Dim PurgeCheckReqd As Boolean = True

		DriveData = SplitDriveString(Drive)
		If DriveData.GetUpperBound(0) = -1 Then
			'There was a problem getting the drive info
			myLogger.PostEntry("Problem splitting drive string " & Drive, ILogger.logMsgType.logError, True)
			Exit Sub
		End If

		Try
			While (PurgeCheckReqd) And (LoopCounter < m_maxRepetitions)			 ' keep checking for purges while there is necessity
				If Not PerformJobPrereqs() Then Exit While
				Select Case SpaceAboveThreshold(Machine, DriveData)
					Case SPACE_CHECK_ERROR					  'Error
						PurgeCheckReqd = False						 'No point in continuing for this disk; error was logged in SpaceAboveThreshold
					Case SPACE_CHECK_ABOVE_THRESHOLD					  'Sufficient available space
						PurgeCheckReqd = False						 'No need for any further action on this disk
						myLogger.PostEntry("No purge required, drive " & DriveData(0), ILogger.logMsgType.logNormal, True)
					Case SPACE_CHECK_BELOW_THRESHOLD					  'Purge required
						myPurgeTask.RequestTask(Machine, DriveData(0))
						If Not myPurgeTask.TaskWasAssigned Then
							PurgeCheckReqd = False							 ' no point to keep checking for purgable datasets
							myLogger.PostEntry("Purge needed; no purge tasks assigned", ILogger.logMsgType.logWarning, False)
						Else
							rp = myStorageOperations.PurgeDataset(myPurgeTask)
							myPurgeTask.CloseTask(rp, myStorageOperations.Message)
						End If
				End Select
				LoopCounter += 1
			End While			  'PurgeCheckReqd
			If LoopCounter >= m_maxRepetitions Then
				myLogger.PostEntry("Max repetition count reached", ILogger.logMsgType.logNormal, True)
			End If
		Catch e As Exception
			Dim Msg As String = "clsMainProcess.DoSpaceManagementForOneDrive(), exception: " & e.Message
			myLogger.PostEntry(Msg, ILogger.logMsgType.logError, True)
		End Try

	End Sub

	Public Sub DoSpaceManagement()

		' do space management function for each of the managed drives on the storage server

		' get list of drives from ini file parameter
		Dim driveList As String = myMgrSettings.GetParam("programcontrol", "drives")

		' get name of machine
		Dim machine As String = myMgrSettings.GetParam("programcontrol", "machname")

		' extract each drive name from list and run the space management process for it
		Dim drives As String() = Split(driveList, ";")
		Dim drive As String
		For Each drive In drives
			DoSpaceManagementForOneDrive(machine, drive)
			myLogger.PostEntry("Space management complete for drive " & drive, ILogger.logMsgType.logNormal, True)
		Next

		myLogger.PostEntry("All space management tasks complete", ILogger.logMsgType.logHealth, False)
		myLogger.PostEntry("===== Closing Space Manager =====", ILogger.logMsgType.logNormal, True)

	End Sub

	Shared Sub Main()
		If IsNothing(m_StartupClass) Then
			m_StartupClass = New clsMainProcess
		End If
		m_StartupClass.DoSpaceManagement()
	End Sub

	Public Sub New()

		DoSetup()
		myLogger.PostEntry("===== Started Space Manager V" & Application.ProductVersion & " ===== ", _
		  myLogger.logMsgType.logNormal, True)

		'Set up FileWatcher to detect setup file changes
		Dim Fi As FileInfo
		Fi = New FileInfo(Application.ExecutablePath)
		m_FileWatcher.BeginInit()
		m_FileWatcher.Path = Fi.DirectoryName
		m_FileWatcher.IncludeSubdirectories = False
		m_FileWatcher.Filter = m_IniFileName
		m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
		m_FileWatcher.EndInit()
		m_FileWatcher.EnableRaisingEvents() = True

	End Sub

	Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed

		'Config file has been changed, so set flag to reread
		m_IniFileChanged = True
		m_FileWatcher.EnableRaisingEvents = False

	End Sub

	Private Function ReReadIniFile() As Boolean

		'Reread the ini file that may have changed
		'Assumes log file and module name remain unchanged

		If Not myMgrSettings.LoadSettings Then Return False

		m_maxRepetitions = Integer.Parse(myMgrSettings.GetParam("programcontrol", "maxrepetitions"))
		m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))
		m_DebugLevel = CInt(myMgrSettings.GetParam("programcontrol", "debuglevel"))

	End Function

	Private Function PerformJobPrereqs() As Boolean

		'Performs all analysis or archive prerequisites have been met before job is executed

		'Check to see if ini file has changed
		If Not CheckIniFileState() Then Return False

		'Is the manager supposed to be active?
		If Not IsMgrActive() Then Return False

		'If we got to here, everything's cool
		Return True

	End Function

	Private Function CheckIniFileState() As Boolean

		'Check to see if the machine settings have changed. Returns True unless the file was changed and 
		'	there was a failure attempting to re-read it
		If m_IniFileChanged Then
			'File has been changed, so re-read it
			If m_DebugLevel > 3 Then
				myLogger.PostEntry("Config file change detected", ILogger.logMsgType.logDebug, True)
			End If
			m_IniFileChanged = False
			If Not ReReadIniFile() Then
				myLogger.PostEntry("Error re-reading ini file", ILogger.logMsgType.logError, True)
				Return False
			End If
			m_FileWatcher.EnableRaisingEvents = True
			'File was successfully read
			Return True
		Else
			'File has not been changed
			Return True
		End If

	End Function

	Private Function IsMgrActive() As Boolean

		'Check to see if manager should still be active
		If m_MgrActive Then
			Return True
		Else
			myLogger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
			Return False
		End If

	End Function

	Private Function SplitDriveString(ByVal DriveStr As String) As String()

		'Splits a string fromatted as "drive,threshold" into a 2-element array containing the drive letter in element 0
		'	and the threshold in element 1. Returns upper bound of -1 if error occurred

		Dim RetArray() As String
		Try
			RetArray = Split(DriveStr, ",")
			If RetArray.GetLength(0) <> 2 Then
				myLogger.PostEntry("Invalid drive string: " & DriveStr, ILogger.logMsgType.logError, True)
				ReDim RetArray(-1)				 'Indicates error
			End If
		Catch ex As Exception
			myLogger.PostEntry("Exception reading drive string: " & DriveStr & "; " & ex.Message, ILogger.logMsgType.logError, True)
			ReDim RetArray(-1)
		End Try

		Return RetArray

	End Function

End Class
