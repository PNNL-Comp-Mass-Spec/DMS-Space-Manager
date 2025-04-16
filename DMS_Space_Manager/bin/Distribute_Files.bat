@echo off

echo Be sure to compile in Release mode
pause

set TargetBase=\\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\SpaceManager

echo Copying to %TargetBase%

@echo on
xcopy Release\net48\Space_Manager.exe                %TargetBase% /D /Y
xcopy Release\net48\Space_Manager.pdb                %TargetBase% /D /Y
xcopy Release\net48\Space_Manager.exe.config         %TargetBase% /D /Y
xcopy Release\net48\*.dll                            %TargetBase% /D /Y /S
xcopy ..\..\README.md                                %TargetBase% /D /Y

:Done

echo.
if not "%1"=="NoPause" pause
