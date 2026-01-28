Packaging (prepared, not built)

- Place icon PNGs into packaging/AppIcon.iconset/
  Required names (macOS iconutil):
    icon_16x16.png
    icon_16x16@2x.png
    icon_32x32.png
    icon_32x32@2x.png
    icon_128x128.png
    icon_128x128@2x.png
    icon_256x256.png
    icon_256x256@2x.png
    icon_512x512.png
    icon_512x512@2x.png

- Create AppIcon.icns (when ready):
    iconutil -c icns AppIcon.iconset

- PyInstaller spec file is provided in this folder.
  Do not build automatically; run manually if/when desired.
