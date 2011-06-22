# -*- coding: utf-8 -*- 

import sys
import os
import xbmc
import xbmcgui
import xbmcaddon

import datetime

__settings__   = xbmcaddon.Addon(id='script.subseek')
__language__   = __settings__.getLocalizedString
__version__    = __settings__.getAddonInfo('version')
__cwd__        = __settings__.getAddonInfo('path')
__profile__    = xbmc.translatePath( __settings__.getAddonInfo('profile') )
__scriptname__ = "SubSeek"
__scriptid__   = "script.subseek"
__author__     = "CSB!"

BASE_RESOURCE_PATH = xbmc.translatePath( os.path.join( __cwd__, 'resources', 'lib' ) )
sys.path.append (BASE_RESOURCE_PATH)
CANCEL_DIALOG  = ( 9, 10, 216, 247, 257, 275, 61467, 61448, )

xbmc.output("### [%s] - Version: %s" % (__scriptname__,__version__,),level=xbmc.LOGDEBUG )

if ( __name__ == "__main__" ):
	if not xbmc.getCondVisibility('Player.Paused') : xbmc.Player().pause() #Pause if not paused
	if xbmc.Player().getSubtitles() == "":
		xbmc.log(__scriptname__+": No Subtitles Currently Loaded. Launching script.xbmc.subtitles", xbmc.LOGDEBUG)
		xbmc.executebuiltin('XBMC.RunPlugin(plugin://script.xbmc.subtitles/)')
	else:
		import gui
		ui = gui.GUI("script-subseek-main.xml" , __cwd__ , "Default")
		ui.doModal()
		del ui
	
		if xbmc.getCondVisibility('Player.Paused'): xbmc.Player().pause()      # if Paused, un-pause
		sys.modules.clear()

