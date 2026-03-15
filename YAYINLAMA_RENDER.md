Render ile En Kolay Yayinlama

1) GitHub'a yukle
- Bu klasoru GitHub reposuna gonder.
- Repo olusturduktan sonra asagidaki komutlari calistir:
  git init
  git add .
  git commit -m "deploy hazirligi"
  git branch -M main
  git remote add origin REPO_URL
  git push -u origin main

2) Render'da otomatik olustur
- Render hesabinda New sec.
- Blueprint secenegini sec.
- GitHub repo bagla.
- Render otomatik olarak render.yaml dosyasini okuyup servisi olusturur.

3) Giris bilgileri
- Ilk deploy sonunda ADMIN_USERNAME degeri admin.
- ADMIN_PASSWORD Render tarafinda otomatik uretilir.
- Render panelinde Environment bolumunden sifreyi gorup giris yap.

4) Veri kaybi olmamasi
- render.yaml icinde kalici disk tanimli.
- Bu sayede data klasoru korunur.

5) Sorun olursa
- Render Logs ekraninda hata satirini kontrol et.
- Uygulama acilmazsa Build ve Start komutlarinin npm install / npm start oldugunu dogrula.
