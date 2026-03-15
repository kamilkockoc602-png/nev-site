Railway ile Yayinlama

1) Hazirlik
- Proje GitHub'da guncel olmali.
- Bu projede calistirma komutu: npm start

2) Railway'de proje olustur
- https://railway.app adresinde oturum ac.
- New Project sec.
- Deploy from GitHub Repo sec.
- nev-site reposunu bagla.

3) Degiskenleri ayarla
- Railway proje ekraninda Variables bolumunu ac.
- Asagidaki degerleri ekle:
  ADMIN_USERNAME=admin
  ADMIN_PASSWORD=guclu_bir_sifre
  DATA_DIR=/data

4) Kalici disk bagla (SQLite icin zorunlu)
- Servis ayarlarinda Volumes bolumune gir.
- Yeni volume olustur.
- Mount Path degeri olarak /data ver.
- Uygulama sqlite dosyasini /data/app.db yolunda tutar.

5) Domain (opsiyonel)
- Settings -> Domains -> Generate Domain ile .up.railway.app domaini al.
- Ozel domain kullanacaksan Add Custom Domain sec ve verilen DNS kayitlarini ekle.

6) Kontrol
- Deploy tamamlandiginda service logs ekraninda hata olmadigini kontrol et.
- Acilis sonrasi giris yapip veri degisikligi yap.
- Restart sonrasi verinin korundugunu dogrula.

Notlar
- PORT degiskenini Railway otomatik verir, manuel girmen gerekmez.
- DATA_DIR degiskeni verilmezse veriler konteyner icinde gecici klasorde kalir.
