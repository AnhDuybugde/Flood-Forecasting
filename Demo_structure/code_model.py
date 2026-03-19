import torch
import torch.nn as nn
import torch.nn.functional as F
import segmentation_models_pytorch as smp

# class ASPP(nn.Module):
#     def __init__(self, in_channels, out_channels):
#         super(ASPP, self).__init__()
#         self.conv1 = nn.Conv2d(in_channels, out_channels, 1, bias=False)
#         self.conv2 = nn.Conv2d(in_channels, out_channels, 3, padding=6, dilation=6, bias=False)
#         self.conv3 = nn.Conv2d(in_channels, out_channels, 3, padding=12, dilation=12, bias=False)
#         self.conv4 = nn.Conv2d(in_channels, out_channels, 3, padding=18, dilation=18, bias=False)
#         self.pool = nn.AdaptiveAvgPool2d(1)
#         self.conv_pool = nn.Conv2d(in_channels, out_channels, 1, bias=False)
#         self.fuse = nn.Sequential(
#             nn.Conv2d(out_channels * 5, out_channels, 1, bias=False),
#             nn.BatchNorm2d(out_channels),
#             nn.ReLU(inplace=True)
#         )

#     def forward(self, x):
#         res = [self.conv1(x), self.conv2(x), self.conv3(x), self.conv4(x)]
#         pooled = self.pool(x)
#         pooled = F.interpolate(self.conv_pool(pooled), size=x.shape[2:], mode='bilinear', align_corners=True)
#         res.append(pooled)
#         return self.fuse(torch.cat(res, dim=1))

# class FloodSOTAUNet_V2(nn.Module):
#     def __init__(self, n_channels=8, n_classes=1):
#         super(FloodSOTAUNet_V2, self).__init__()
#         self.base_model = smp.Unet(
#             encoder_name="efficientnet-b4", 
#             encoder_weights=None, # Khi load checkpoint không cần tải weights imagenet
#             in_channels=n_channels,
#             classes=n_classes,
#             decoder_attention_type='scse'
#         )
#         self.aspp = ASPP(448, 448)
#         self.tide_embedding = nn.Sequential(
#             nn.Linear(1, 128), nn.SiLU(),
#             nn.Linear(128, 448), nn.Sigmoid()
#         )
#         self.input_gate = nn.Sequential(
#             nn.AdaptiveAvgPool2d(1),
#             nn.Conv2d(n_channels, n_channels, 1),
#             nn.Sigmoid()
#         )

#     def forward(self, x):
#         x = x * self.input_gate(x)
#         features = self.base_model.encoder(x)
#         tide_val = x[:, 4, 0, 0].unsqueeze(1) 
#         tide_context = self.tide_embedding(tide_val).view(-1, 448, 1, 1)
#         bottleneck = self.aspp(features[-1])
#         features[-1] = bottleneck * tide_context 
#         decoder_output = self.base_model.decoder(features)
#         return self.base_model.segmentation_head(decoder_output)


class ASPP(nn.Module):
    def __init__(self, in_dims, out_dims):
        super().__init__()
        self.conv1 = nn.Conv2d(in_dims, out_dims, 1, bias=False)
        self.conv2 = nn.Conv2d(in_dims, out_dims, 3, padding=6, dilation=6, bias=False)
        self.conv3 = nn.Conv2d(in_dims, out_dims, 3, padding=12, dilation=12, bias=False)
        self.conv_pool = nn.Sequential(nn.AdaptiveAvgPool2d(1), nn.Conv2d(in_dims, out_dims, 1, bias=False))
        self.fuse = nn.Sequential(nn.Conv2d(out_dims*4, out_dims, 1), nn.BatchNorm2d(out_dims), nn.ReLU())

    def forward(self, x):
        h, w = x.shape[-2:]
        res = [
            self.conv1(x),
            self.conv2(x),
            self.conv3(x),
            torch.nn.functional.interpolate(self.conv_pool(x), size=(h,w), mode='bilinear', align_corners=True)
        ]
        return self.fuse(torch.cat(res, dim=1))

class FlexibleFloodModel(nn.Module):
    def __init__(self, encoder_name='resnet101'):
        super().__init__()
        self.base = smp.Unet(
            encoder_name=encoder_name,
            encoder_weights=None,
            in_channels=8,
            classes=1,
            decoder_attention_type='scse' 
        )
        ch = self.base.encoder.out_channels[-1]
        self.aspp = ASPP(ch, ch)

    def forward(self, x):
        feats = self.base.encoder(x)
        feats[-1] = self.aspp(feats[-1])
        dec = self.base.decoder(feats)
        return self.base.segmentation_head(dec)