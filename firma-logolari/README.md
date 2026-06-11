# Firma Logoları

Bu klasöre firma logolarını PNG/JPG formatında ekleyebilirsiniz.

## Dosya adı kuralı

Firma adı **slug** formatında dosya adı olarak kullanılır:

| Firma Adı | Dosya Adı |
|-----------|-----------|
| Kamil Koç | `kamil-koc.png` |
| Enver Geçgel Turizm | `enver-gecgel-turizm.png` |
| Niğde Aydoğanlar | `nigde-aydoganlar.png` |
| Has Karayolu Turizm | `has-karayolu-turizm.png` |

### Normalize kuralları
- Tüm harfler küçük
- Türkçe karakterler dönüşür: ı→i, ş→s, ğ→g, ü→u, ö→o, ç→c
- Boşluklar tire ile değişir
- Diğer özel karakterler silinir

## Boyut önerisi
- 128×128 piksel (kare)
- PNG (şeffaf arka plan ideal)

## Desteklenen uzantılar
Sistem sırayla dener: `.png` → `.jpg` → `.jpeg` → `.webp` → `.svg`
Yani dosya `kamil-koc.png` de olabilir `kamil-koc.jpeg` de — sistem bulur.

## ⚠️ Windows kullanıcıları için kritik
Windows varsayılan olarak dosya uzantısını gizler. "Yeniden Adlandır"
deyip `kamil-koc.png` yazınca gerçek ad `kamil-koc.png.jpeg` olur.
Çözüm: **Dosya gezgini → Görünüm → Dosya uzantıları** kutusunu açın,
gerçek uzantıyı görebilirsiniz.

## Logo yoksa
Logo dosyası bulunamayan firmalar için **otomatik renkli avatar**
(firma adının ilk harfleri ile) gösterilir.
