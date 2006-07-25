Imports System.IO
Imports System.Management
Imports SpaceManagerNet.MgrSettings
'Imports SpaceManagerNet.Logging
Imports PRISM.Logging
Imports SpaceManagerNet.PurgeTask
Imports SpaceManagerNet.UnpurgeTask


Public Class clsMainProcess

#Region "Member Variables"
  Private myMgrSettings As clsSpaceMgrSettings
  Private myLogger As ILogger
  Private myPurgeTask As clsPurgeTask
  Private myUnpurgeTask As clsUnpurgeTask
  Private myStorageOperations As clsStorageOperations

  Private m_spaceThreshold As Long
  Private m_clientPerspective As Boolean = False
	Private m_maxRepetitions As Integer = 1
	Private Shared m_StartupClass As clsMainProcess
	Private m_IniFileChanged As Boolean = False
	Private WithEvents m_FileWatcher As New FileSystemWatcher()
	Private m_IniFileName As String = "SpaceManager.xml"
	Private m_MgrActive As Boolean = True
	Private m_JobsFound As Boolean
#End Region

  ' create full path to ini file
  Private Function GetIniFilePath(ByVal IniFileName As String) As String
    Dim fi As New FileInfo(Application.ExecutablePath)
		Return Path.Combine(fi.DirectoryName, IniFileName)
  End Function

  ' create and initialize the objects needed to do space management
	Private Sub DoSetup()
		' create the object that will manage the analysis manager parameters
		'
		myMgrSettings = New clsSpaceMgrSettings(GetIniFilePath(m_IniFileName))

		' create the object that will manage the logging
		'
		Dim dbl As New clsDBLogger()
		dbl.LogFilePath = myMgrSettings.GetParam("logging", "logfilename")
		dbl.ConnectionString = myMgrSettings.GetParam("DatabaseSettings", "ConnectionString")
		dbl.ModuleName = myMgrSettings.GetParam("programcontrol", "modulename")
		dbl.LogExecutableName = False
		dbl.LogExecutableVersion = False
		myLogger = New clsQueLogger(dbl)
		dbl = Nothing

		' create the object that will manage the purge tasks
		'
		myPurgeTask = New clsPurgeTask(myMgrSettings, myLogger)

		' create the object that will manage the unpurge tasks
		'
		myUnpurgeTask = New clsUnpurgeTask(myMgrSettings, myLogger)

		' create the object that will manager storage space operations
		'
		myStorageOperations = New clsStorageOperations(myMgrSettings, myLogger)

		' get space threshold
		'
		m_spaceThreshold = Long.Parse(myMgrSettings.GetParam("programcontrol", "minfreegigabytes"))

		' client or server perspective?
		'
		m_clientPerspective = (myMgrSettings.GetParam("programcontrol", "perspective") = "client")

		' maximum number of passes through the main purge loop
		'
		m_maxRepetitions = Integer.Parse(myMgrSettings.GetParam("programcontrol", "maxrepetitions"))

		' Manager active?
		'
		m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))

	End Sub

	' determine if free space on disk is above acceptable threshold
	Private Function SpaceAboveThreshold(ByVal machine As String, ByVal drive As String) As Boolean
		Dim availableSpace As Double
		Dim request As String

		' get WMI object representing drive and populate it
		' and use it to get free space and convert result to gigabytes
		'
		If m_clientPerspective Then
			request = "\\" & machine & "\root\cimv2:win32_logicaldisk.deviceid=""" & drive & """"
		Else
			request = "win32_logicaldisk.deviceid=""" & drive & """"
		End If
		Dim disk As New ManagementObject(request)
		disk.Get()
		availableSpace = System.Convert.ToDouble(disk("FreeSpace"))
		availableSpace /= 2 ^ 30		'convert raw byte count to gigabytes

		Return availableSpace > m_spaceThreshold
	End Function

	' Manage the storage space for the given drive on the storage server
	Public Sub DoSpaceManagementForOneDrive(ByVal machine As String, ByVal drive As String)
		Dim rp As PurgeTask.IPurgeTaskParams.CloseOutType
		Dim ru As UnpurgeTask.IUnpurgeTaskParams.CloseOutType

		'TODO: Implement loop counter for repeated manager runs (may not be necessary for this manager)
		Dim loopCounter As Integer = 0
		'TODO: If loop counter implemented, add check for changed machine settings and mgr active setting

		Dim PurgeCheckReqd As Boolean = True
		Dim UnpurgeCheckReqd As Boolean = True

		Try
			While UnpurgeCheckReqd		' keep checking for unpurges while there is necessity
				While PurgeCheckReqd		 ' keep checking for purges while there is necessity
					If SpaceAboveThreshold(machine, drive) Then
						PurgeCheckReqd = False					' we don't have to keep checking for purgable datasets
						myLogger.PostEntry("No purge required", ILogger.logMsgType.logNormal, True)
					Else
						myPurgeTask.RequestTask(drive)
						If Not myPurgeTask.TaskWasAssigned Then
							PurgeCheckReqd = False					 ' no point to keep checking for purgable datasets
							myLogger.PostEntry("Purge needed; no purge tasks assigned", ILogger.logMsgType.logWarning, False)
						Else
							rp = myStorageOperations.PurgeDataset(myPurgeTask)
							myPurgeTask.CloseTask(rp, myStorageOperations.Message)
						End If
					End If
				End While		 'PurgeCheckReqd

				myUnpurgeTask.RequestTask(drive)
				If Not myUnpurgeTask.TaskWasAssigned Then
					UnpurgeCheckReqd = False				' no need to keep checking
					myLogger.PostEntry("No unpurge required", ILogger.logMsgType.logNormal, True)
				Else
					ru = myStorageOperations.UnpurgeDataset(myUnpurgeTask)
					myUnpurgeTask.CloseTask(ru, myStorageOperations.Message)
					PurgeCheckReqd = True			' we need to go back and check our free space
				End If
			End While		'UnpurgeCheckReqd
		Catch e As Exception
			'' future
		End Try

	End Sub

	' do space management function for each of the managed drives on the storage server
	Public Sub DoSpaceManagement()

		'If manager inactive, then exit program
		If Not m_MgrActive Then
			myLogger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
			myLogger.PostEntry("===== Closing Space Manager =====", ILogger.logMsgType.logNormal, True)
			Exit Sub
		End If

		' get name of machine
		'
		Dim machine As String = myMgrSettings.GetParam("programcontrol", "machname")

		' get list of drives from ini file parameter
		'
		Dim driveList As String = myMgrSettings.GetParam("programcontrol", "drives")

		' extract each drive name from list and run the space management process for it
		'
		Dim drives As String() = Split(driveList, ",")
		Dim drive As String
		For Each drive In drives
			DoSpaceManagementForOneDrive(machine, drive)
			myLogger.PostEntry("Space management complete for drive " & drive, ILogger.logMsgType.logNormal, True)
		Next

		myLogger.PostEntry("All space management tasks complete", ILogger.logMsgType.logHealth, False)
		myLogger.PostEntry("===== Closing Space Manager =====", ILogger.logMsgType.logNormal, True)

		RaiseEvent StatusChange("Space management complete")

	End Sub

	Shared Sub Main()
		If IsNothing(m_StartupClass) Then
			m_StartupClass = New clsMainProcess()
		End If
		m_StartupClass.DoSpaceManagement()
	End Sub

	Public Sub New()

		Dim Fi As FileInfo

		DoSetup()
		myLogger.PostEntry("===== Started Space Manager V" & Application.ProductVersion & " ===== ", _
		myLogger.logMsgType.logNormal, True)

		'Set up FileWatcher to detect setup file changes
		Fi = New FileInfo(Application.ExecutablePath)
		m_FileWatcher.BeginInit()
		m_FileWatcher.Path = Fi.DirectoryName
		m_FileWatcher.IncludeSubdirectories = False
		m_FileWatcher.Filter = m_IniFileName
		m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
		m_FileWatcher.EndInit()
		m_FileWatcher.EnableRaisingEvents() = True

	End Sub

	Public Event StatusChange(ByVal NewStatus As String)


	Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed
		m_IniFileChanged = True
		m_FileWatcher.EnableRaisingEvents = False
	End Sub

	Private Function ReReadIniFile() As Boolean

		'Reread the ini file that may have changed
		'Assumes log file and module name remain unchanged

		If Not myMgrSettings.LoadSettings Then Return False

		m_spaceThreshold = Long.Parse(myMgrSettings.GetParam("programcontrol", "minfreegigabytes"))
		m_clientPerspective = (myMgrSettings.GetParam("programcontrol", "perspective") = "client")
		m_maxRepetitions = Integer.Parse(myMgrSettings.GetParam("programcontrol", "maxrepetitions"))
		m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))

	End Function

End Class
